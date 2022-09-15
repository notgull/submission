//! Interface to io_uring.

use super::Completion;

use async_io::Async;
use futures_lite::prelude::*;
use io_uring::types::{Fd, SubmitArgs, Timespec};
use io_uring::{cqueue::Entry as CompleteEntry, opcode, squeue::Entry as SubmitEntry, IoUring};

use std::fmt;
use std::io::{self, Write as _};
use std::mem::MaybeUninit;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{
    AtomicBool,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::Mutex;

macro_rules! syscall {
    ($ident: ident $($arg: tt)*) => {
        match unsafe { libc::$ident $($arg)* } {
            ret if ret < 0 => Err(io::Error::last_os_error()),
            ret => Ok(ret),
        }
    }
}

pub(crate) struct Ring {
    /// The actual io_uring instance.
    instance: IoUring,

    /// The guard for the submission queue.
    ///
    /// Holding this lock implies the exclusive right to submit events.
    submit: Mutex<()>,

    /// The guard for the completion queue.
    ///
    /// Holding this lock implies the exclusive right to poll for completions.
    complete: Mutex<()>,

    /// Notifications for when new completion events are available on the ring.
    new_events: Async<EventFd>,

    /// Are we currently waiting for completion events?
    waiting: AtomicBool,

    /// Are we currently notified?
    notified: AtomicBool,
}

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ring")
            .field("waiting", &self.waiting.load(Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) struct Operation {
    /// The submission entry for this operation.
    submit: Option<SubmitEntry>,
}

pub(crate) struct Events {
    /// Buffer for the completion events.
    buffer: Box<[MaybeUninit<CompleteEntry>; 1024]>,

    /// The number of events in the buffer.
    count: usize,
}

impl Ring {
    pub(crate) fn new() -> io::Result<Self> {
        let instance = IoUring::new(1024)?;
        let new_events = EventFd::new()?;
        instance.submitter().register_eventfd(new_events.0)?;

        Ok(Self {
            instance,
            submit: Mutex::new(()),
            complete: Mutex::new(()),
            new_events: Async::new(new_events)?,
            waiting: AtomicBool::new(false),
            notified: AtomicBool::new(false),
        })
    }

    pub(crate) fn events(&self) -> io::Result<Events> {
        Ok(Events::new())
    }

    pub(crate) fn register(&self, _fd: RawFd) -> io::Result<()> {
        // This does nothing for now; in the future, we may want to use the `IORING_REGISTER_FILES`
        // opcode to register the file descriptor with the ring.

        Ok(())
    }

    pub(crate) fn deregister(&self, _fd: RawFd) -> io::Result<()> {
        // This does nothing for now; in the future, we may want to use the `IORING_UNREGISTER_FILES`
        // opcode to deregister the file descriptor with the ring.

        Ok(())
    }

    pub(crate) unsafe fn submit(&self, mut operation: Pin<&mut Operation>) -> io::Result<()> {
        log::trace!("submission: submitting operation");

        // Get the submission entry.
        let s_entry = operation.submit.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "operation has already been submitted")
        })?;

        // Acquire a lock on the submission queue.
        let _guard = self.submit.lock().unwrap();

        // Submit the operation.
        // SAFETY: We have exclusive access to the submission queue.
        let mut queue = unsafe { self.instance.submission_shared() };

        // SAFETY: The caller ensures the operation is valid.
        unsafe {
            queue
                .push(&s_entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to submit operation"))?;
        }

        // If we're currently waiting for completion events, submit the queue.
        if self.waiting.load(SeqCst) {
            // Notify the upper loop that we may need to submit again.
            self.notify()?;
        }

        Ok(())
    }

    pub(crate) fn cancel(&self, _op: Pin<&mut Operation>, key: u64) -> io::Result<()> {
        // Acquire a lock on the submission queue.
        let _guard = self.submit.lock().unwrap();

        // Cancel the operation.
        // SAFETY: We have exclusive access to the submission queue.
        let mut queue = unsafe { self.instance.submission_shared() };
        let cancel_op = opcode::AsyncCancel::new(key).build();

        // SAFETY: A cancel entry is always valid.
        unsafe {
            queue
                .push(&cancel_op)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to cancel operation"))?;
        }

        if self.waiting.load(SeqCst) {
            // Submit the cancellation to the running process.
            self.instance.submitter().submit()?;
        }

        Ok(())
    }

    #[allow(clippy::await_holding_lock)]
    pub(crate) async fn wait(&self, events: &mut Events) -> io::Result<usize> {
        // Indicate that we are waiting.
        let _guard = CallOnDrop(|| {
            self.waiting.store(false, SeqCst);
        });
        self.waiting.store(true, SeqCst);

        loop {
            match self.poll_for_events(events)? {
                0 => {
                    // Wait for new events.
                    let mut buffer = [0; 8];
                    (&self.new_events).read(&mut buffer).await?;

                    // We are no longer notified.
                    self.notified.store(false, SeqCst);
                },
                len => return Ok(len),
            }
        }
    }

    /// Inner function to poll for events.
    fn poll_for_events(&self, events: &mut Events) -> io::Result<usize> {
        // See if there are any new events.
            // Poll with a zero timeout.
            let timeout = Timespec::new().sec(0).nsec(0);
            let args = SubmitArgs::new().timespec(&timeout);
            if let Err(e) = self.instance.submitter().submit_with_args(1, &args) {
                match e.raw_os_error() {
                    Some(62) => {
                        // The timer expired, this is intentional,
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }

            // Acquire a lock on the completion queue.
            let _queue_guard = self.complete.lock().unwrap();

            // Poll for completions.
            // SAFETY: We have exclusive access to the completion queue.
            let mut queue = unsafe { self.instance.completion_shared() };
            if queue.is_empty() {
                // The queue is empty. Wait for completion or a new submission.
                log::trace!("submission: waiting for new events");
                return Ok(0);
            } else {
                log::trace!("submission: found {} events", queue.len());
                // The queue is not empty, begin emptying out events.
                let filled = queue.fill(&mut *events.buffer);
                let len = filled.len();
                events.count = len;
                return Ok(len);
            }
    }

    fn notify(&self) -> io::Result<()> {
        if self.notified.swap(true, SeqCst) {
            // This should never block.
            let buffer = 1u64.to_ne_bytes();
            let _ = self.new_events.get_ref().write(&buffer)?;
        }

        Ok(())
    }
}

impl Events {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Box::new(unsafe { MaybeUninit::uninit().assume_init() }),
            count: 0,
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Completion> + '_ {
        self.buffer[..self.count]
            .iter()
            .map(|entry| unsafe { &*(entry as *const _ as *const CompleteEntry) })
            .map(|entry| Completion {
                key: entry.user_data(),
                result: Some(match entry.result() {
                    result if result < -1 => Err(io::Error::from_raw_os_error(-result)),
                    result => Ok(result as isize),
                }),
            })
    }
}

impl Operation {
    pub(crate) unsafe fn read(fd: RawFd, buf: *mut u8, len: usize, offset: u64, key: u64) -> Self {
        opcode::Read::new(Fd(fd), buf, len as _)
            .offset(offset as _)
            .build()
            .user_data(key)
            .into()
    }

    pub(crate) unsafe fn write(fd: RawFd, buf: *mut u8, len: usize, offset: u64, key: u64) -> Self {
        opcode::Write::new(Fd(fd), buf, len as _)
            .offset(offset as _)
            .build()
            .user_data(key)
            .into()
    }
}

impl From<SubmitEntry> for Operation {
    fn from(entry: SubmitEntry) -> Self {
        Self {
            submit: Some(entry),
        }
    }
}

/// A wrapper around the `EventFd` used to notify us of new events.
///
/// Use `OwnedFd` once we bump our MSRV to 1.63.
struct EventFd(RawFd);

impl EventFd {
    fn new() -> io::Result<Self> {
        let fd = syscall!(eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK))?;
        Ok(EventFd(fd))
    }
}

impl io::Read for EventFd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        syscall!(read(self.0, buf.as_mut_ptr() as *mut _, buf.len()))?;
        Ok(buf.len())
    }
}

impl io::Read for &EventFd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        syscall!(read(self.0, buf.as_mut_ptr() as *mut _, buf.len()))?;
        Ok(buf.len())
    }
}

impl io::Write for EventFd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        syscall!(write(self.0, buf.as_ptr() as *const _, buf.len()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Write for &EventFd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        syscall!(write(self.0, buf.as_ptr() as *const _, buf.len()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
