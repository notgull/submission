//! Interface to IOCP.

use super::Completion;

use async_lock::Mutex;
use blocking::Unblock;
use concurrent_queue::ConcurrentQueue;
use futures_lite::{future, prelude::*};

use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::marker::PhantomPinned;
use std::mem::{self, zeroed, ManuallyDrop, MaybeUninit};
use std::os::windows::io::RawHandle;
use std::pin::Pin;
use std::ptr;

use windows_sys::Win32::Foundation as found;
use windows_sys::Win32::Storage::FileSystem as files;
use windows_sys::Win32::System::WindowsProgramming as prog;
use windows_sys::Win32::System::IO as wio;

const WAKEUP_KEY: usize = std::usize::MAX;
const SHUTDOWN_KEY: usize = std::usize::MAX - 1;

pub(crate) struct Ring {
    /// The raw handle to the IO completion port.
    ///
    /// Logically, this is owned by `EventLoop`, so we don't drop it.
    port: ManuallyDrop<CompletionPort>,

    /// Event loop governing events.
    ///
    /// We keep this running in the `blocking` thread pool. Logically it
    /// is an iterator so that it will keep running and accumulating events
    /// until it has too many, in which case the thread is freed up for
    /// other work.
    event_loop: Mutex<Unblock<EventLoop>>,

    /// Event notifications for fixed events.
    ///
    /// These are pinned to the heap.
    notifications: Pin<Box<Notifications>>,

    /// Events that completed early.
    early_events: ConcurrentQueue<Completion>,
}

unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ring")
            .field("early_events", &self.early_events.len())
            .finish_non_exhaustive()
    }
}

struct Notifications {
    /// Entry for waking up the thread and asking for more events.
    wakeup: UnsafeCell<wio::OVERLAPPED>,

    /// Entry for shutting down the thread.
    shutdown: UnsafeCell<wio::OVERLAPPED>,
}

impl Ring {
    pub(crate) fn new() -> io::Result<Self> {
        const MAX_EVENTS: usize = 2;

        let port = CompletionPort::new()?;

        // Spawn a task for polling for events on the blocking threadpool.
        let event_loop = Unblock::with_capacity(MAX_EVENTS, EventLoop::new(&port)?);

        Ok(Self {
            port: ManuallyDrop::new(port),
            event_loop: Mutex::new(event_loop),
            notifications: Box::pin(Notifications {
                wakeup: UnsafeCell::new(unsafe { zeroed() }),
                shutdown: UnsafeCell::new(unsafe { zeroed() }),
            }),
            early_events: ConcurrentQueue::unbounded(),
        })
    }

    pub(crate) fn events(&self) -> io::Result<Events> {
        Ok(Events(None))
    }

    pub(crate) fn register(&self, fd: RawHandle) -> io::Result<RawHandle> {
        // Reopen the handle so that it supports overlapped I/O.
        let handle = fd as found::HANDLE;

        let result = unsafe {
            files::ReOpenFile(
                handle,
                files::FILE_GENERIC_READ | files::FILE_GENERIC_WRITE,
                files::FILE_SHARE_READ | files::FILE_SHARE_WRITE | files::FILE_SHARE_DELETE,
                files::FILE_FLAG_OVERLAPPED,
            )
        };

        if result == found::INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        self.port.associate(result, 0)?;

        // TODO(notgull): Use strict provenance once it is stabilized.
        Ok(result as _)
    }

    pub(crate) fn deregister(&self, _fd: RawHandle) -> io::Result<()> {
        // Does nothing for now.
        Ok(())
    }

    pub(crate) unsafe fn submit(&self, operation: Pin<&mut Operation>) -> io::Result<()> {
        let project = operation.project();

        // Try the fast path first.
        if let Some(res) = project
            .ty
            .run(
                *project.handle,
                project.overlapped.get_unchecked_mut(),
                *project.buf,
                *project.len,
            )
            .transpose()
        {
            // We got an early result. Push it into our queue.
            self.early_events
                .push(Completion {
                    key: *project.key,
                    result: Some(res),
                })
                .ok();
        }

        Ok(())
    }

    pub(crate) fn cancel(&self, operation: Pin<&mut Operation>, _key: u64) -> io::Result<()> {
        // Run CancelIoEx to cancel this specific operation.
        let operation = operation.project();
        let handle = *operation.handle;

        let result = unsafe { wio::CancelIoEx(handle, operation.overlapped.get_unchecked_mut()) };

        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn steal_early(&self, events: &mut Vec<Completion>) -> usize {
        let mut pushed = 0;

        if !self.early_events.is_empty() {
            events.reserve(self.early_events.len());

            while let Ok(early_event) = self.early_events.pop() {
                events.push(early_event);
                pushed += 1;
            }
        }

        pushed
    }

    pub(crate) async fn wait(&self, events: &mut Events, park: bool) -> io::Result<usize> {
        // We are looking for the first non-empty event cache.
        let mut event_loop = self.event_loop.lock().await;
        let mut resolve_event = event_loop.find(|event| event.actual != 0);

        // Poll it once to see if it's already ready.
        match future::poll_once(&mut resolve_event).await {
            Some(Some(new_events)) => {
                // One is already available, we're done.
                let len = new_events.actual;
                events.0 = Some(new_events);
                return Ok(len);
            }
            Some(None) => {
                unreachable!("Event loop terminated")
            }
            None => {
                // First poll returned Poll::Pending, we need to notify the reactor.
            }
        }

        // Return if we don't want to park.
        if !park {
            return Ok(0);
        }

        // Notify the event loop so that it returns early.
        unsafe {
            self.port
                .notify(WAKEUP_KEY, self.notifications.wakeup.get())?;
        }

        // Wait for the event loop to return.
        match resolve_event.await {
            Some(new_events) => {
                let len = new_events.actual;
                events.0 = Some(new_events);
                return Ok(len);
            }
            None => panic!("event loop terminated"),
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // Notify the event loop to shut down.
        unsafe {
            self.port
                .notify(SHUTDOWN_KEY, self.notifications.shutdown.get())
                .unwrap();
        }
    }
}

/// This needs `Option` so we can move it out and send it.
pub(crate) struct Events(Option<InnerEvents>);

impl Events {
    pub(crate) fn iter(&self) -> impl Iterator<Item = Completion> + '_ {
        self.0
            .iter()
            .flat_map(|inner| inner.entries(inner.len))
            .filter_map(|entry| {
                if matches!(entry.lpCompletionKey, WAKEUP_KEY | SHUTDOWN_KEY) {
                    None
                } else {
                    // Get the pointer to the overlapped structure.
                    let overlapped = unsafe { &*(entry.lpOverlapped) };

                    // Get the result from the OVERLAPPED structure.
                    let result = if overlapped.Internal as u32 == found::ERROR_SUCCESS {
                        Ok(overlapped.InternalHigh as _)
                    } else {
                        cvt_res(overlapped.Internal as _)
                            .map_err(|_| io::Error::from_raw_os_error(overlapped.Internal as _))
                    };

                    // Cast the OVERLAPPED point to an Operation to get our key.
                    let operation = unsafe { &*(entry.lpOverlapped as *mut Operation) };

                    Some(Completion {
                        key: operation.key,
                        result: Some(result),
                    })
                }
            })
    }
}

struct InnerEvents {
    /// Buffer of overlapped entries.
    buffer: Box<[MaybeUninit<wio::OVERLAPPED_ENTRY>; 1024]>,

    /// The number of entries in the buffer.
    len: usize,

    /// The number of actual entries in the buffer.
    ///
    /// "Actual" entries are defined as non-wakeup or non-shutdown.
    actual: usize,
}

unsafe impl Send for InnerEvents {}
unsafe impl Sync for InnerEvents {}

impl InnerEvents {
    fn new() -> Self {
        Self {
            buffer: Box::new(unsafe { MaybeUninit::uninit().assume_init() }),
            len: 0,
            actual: 0,
        }
    }

    /// List of actual valid entries.
    fn valid(&self) -> &[wio::OVERLAPPED_ENTRY] {
        unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const wio::OVERLAPPED_ENTRY,
                self.len,
            )
        }
    }

    fn open_space(&mut self) -> &mut [MaybeUninit<wio::OVERLAPPED_ENTRY>] {
        &mut self.buffer[self.len..]
    }

    /// Iterate over valid entries.
    fn entries(&self, tail: usize) -> impl Iterator<Item = &wio::OVERLAPPED_ENTRY> {
        let start = self.len.saturating_sub(tail);
        self.valid()[start..].iter()
    }
}

pin_project_lite::pin_project! {
    #[repr(C)]
    pub(crate) struct Operation {
        // The OVERLAPPED structure for operation data storage.
        #[pin]
        overlapped: wio::OVERLAPPED,

        // The raw handle to the IO completion port.
        handle: found::HANDLE,

        // Pointer to the buffer.
        buf: *mut u8,

        // Length of buffer.
        len: u32,

        // The type of this operation.
        ty: OpType,

        // The key associated with this operation.
        key: u64,

        #[pin]
        _pin: PhantomPinned
    }
}

unsafe impl Send for Operation {}

impl Operation {
    pub(crate) fn read(fd: RawHandle, buf: *mut u8, len: usize, offset: u64, key: u64) -> Self {
        Self {
            overlapped: overlapped_offset(offset),
            handle: fd as _,
            buf,
            len: len as _,
            ty: OpType::Read,
            key,
            _pin: PhantomPinned,
        }
    }

    pub(crate) fn write(fd: RawHandle, buf: *mut u8, len: usize, offset: u64, key: u64) -> Self {
        Self {
            overlapped: overlapped_offset(offset),
            handle: fd as _,
            buf,
            len: len as _,
            ty: OpType::Write,
            key,
            _pin: PhantomPinned,
        }
    }
}

#[derive(Copy, Clone)]
enum OpType {
    Read,
    Write,
}

impl OpType {
    /// Run this operation on the given handle.
    ///
    /// Returns `Some` if the operation completed immediately.
    unsafe fn run(
        &self,
        handle: found::HANDLE,
        overlapped: *mut wio::OVERLAPPED,
        buffer: *mut u8,
        len: u32,
    ) -> io::Result<Option<isize>> {
        let result = match self {
            OpType::Read => {
                log::trace!("syscall: ReadFile, handle={:x}, buf={:p}, len={}, out_len=null, overlapped={:p}", handle, buffer, len, overlapped);
                files::ReadFile(handle, buffer.cast(), len, ptr::null_mut(), overlapped)
            }
            OpType::Write => {
                log::trace!("syscall: WriteFile, handle={:x}, buf={:p}, len={}, out_len=null, overlapped={:p}", handle, buffer, len, overlapped);
                files::WriteFile(
                    handle,
                    buffer.cast() as *const _,
                    len,
                    ptr::null_mut(),
                    overlapped,
                )
            }
        };

        if result == 0 {
            // If the error code is ERROR_IO_PENDING, then the operation is
            // pending and we should return `None`.
            if found::GetLastError() == found::ERROR_IO_PENDING {
                return Ok(None);
            }

            cvt_res(result).map(Some)
        } else {
            // Tell how many bytes were written.
            todo!("check if this is correct")
        }
    }
}

fn cvt_res(_result: found::BOOL) -> io::Result<isize> {
    let error = unsafe { found::GetLastError() };

    // If the error is ERROR_HANDLE_EOF, just return zero bytes.
    if error == found::ERROR_HANDLE_EOF {
        Ok(0)
    } else {
        Err(io::Error::last_os_error())
    }
}

/// The actual I/O completion port.
#[derive(Clone)]
struct CompletionPort(found::HANDLE);

unsafe impl Send for CompletionPort {}
unsafe impl Sync for CompletionPort {}

impl CompletionPort {
    /// Create a new, unassociated `CompletionPort`.
    fn new() -> io::Result<Self> {
        log::trace!("syscall: CreateIoCompletionPort (creation)");
        let handle = unsafe { wio::CreateIoCompletionPort(found::INVALID_HANDLE_VALUE, 0, 0, 0) };

        if handle == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }

    /// Associate a handle with this completion port.
    fn associate(&self, handle: found::HANDLE, key: usize) -> io::Result<()> {
        log::trace!("syscall: CreateIoCompletionPort (associate)");
        let handle = unsafe { wio::CreateIoCompletionPort(handle, self.0, key, 0) };

        if handle == 0 {
            Err(io::Error::last_os_error())
        } else {
            debug_assert_eq!(handle, self.0);
            Ok(())
        }
    }

    /// Wait for events from this `CompletionPort`.
    fn wait(&self, events: &mut InnerEvents) -> io::Result<usize> {
        // Stack space for results.
        let mut result_entries = MaybeUninit::uninit();
        let space = events.open_space();

        // Call the function.
        log::trace!("syscall: GetQueuedCompletionStatusEx");
        let res = unsafe {
            wio::GetQueuedCompletionStatusEx(
                self.0,
                space.as_mut_ptr() as _,
                space.len() as _,
                result_entries.as_mut_ptr(),
                prog::INFINITE,
                0,
            )
        };

        // Check if the call succeeded.
        if res == 0 {
            return Err(io::Error::last_os_error());
        }

        // Get the number of results.
        let result_entries = unsafe { result_entries.assume_init() };

        // Update the number of events.
        events.len += result_entries as usize;
        events.actual += result_entries as usize;

        Ok(result_entries as usize)
    }

    /// Notify the completion port with a specific event key and OVERLAPPED.
    unsafe fn notify(&self, key: usize, overlapped: *mut wio::OVERLAPPED) -> io::Result<()> {
        log::trace!("syscall: PostQueuedCompletionStatus");
        let res = wio::PostQueuedCompletionStatus(self.0, 0, key, overlapped);

        if res == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for CompletionPort {
    fn drop(&mut self) {
        unsafe {
            found::CloseHandle(self.0);
        }
    }
}

/// The streaming iterator that governs the event loop.
struct EventLoop {
    /// The completion port.
    port: CompletionPort,

    /// The current event buffer.
    events: InnerEvents,

    /// Have we been shut down?
    shutdown: bool,
}

unsafe impl Send for EventLoop {}

impl EventLoop {
    fn new(port: &CompletionPort) -> io::Result<Self> {
        Ok(Self {
            port: port.clone(),
            events: InnerEvents::new(),
            shutdown: false,
        })
    }
}

impl Iterator for EventLoop {
    type Item = InnerEvents;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we've already been shutdown, return None.
            if self.shutdown {
                return None;
            }

            // Wait for events.
            let new_events = match self.port.wait(&mut self.events) {
                Ok(new_events) => new_events,
                Err(e) => {
                    log::error!("Error occurred while waiting for events: {}", e);
                    continue;
                }
            };

            log::trace!(
                "EventLoop: finished wait for new status events, new_events={}",
                new_events
            );

            // Run over the events we've received and see if there are any signals.
            let mut total_notifications = 0;

            for entry in self.events.entries(new_events) {
                log::trace!("Entry Key: {}", entry.lpCompletionKey);
                if entry.lpCompletionKey == WAKEUP_KEY {
                    // Don't notify if this is the only event.
                    log::trace!("EventLoop: received wakeup event");
                    if self.events.actual != 1 {
                        total_notifications += 1;
                    }
                } else if entry.lpCompletionKey == SHUTDOWN_KEY {
                    log::trace!("EventLoop: received shutdown event");
                    self.shutdown = true;
                    return None;
                }
            }

            // If we're notified or if we have too many entries, return the buffer.
            log::trace!(
                "EventLoop: found {} total notifications",
                total_notifications
            );
            if total_notifications > 0 || self.events.len >= self.events.buffer.len() {
                self.events.actual = self.events.actual.saturating_sub(total_notifications);
                let events = mem::replace(&mut self.events, InnerEvents::new());

                return Some(events);
            }

            // Continue waiting.
        }
    }
}

impl std::iter::FusedIterator for EventLoop {}

fn overlapped_offset(offset: u64) -> wio::OVERLAPPED {
    let mut overlapped: wio::OVERLAPPED = unsafe { zeroed() };

    let offset_container = wio::OVERLAPPED_0_0 {
        Offset: offset as _,
        OffsetHigh: (offset >> 32) as _,
    };

    // Set the union field.
    overlapped.Anonymous.Anonymous = offset_container;

    overlapped
}
