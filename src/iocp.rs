//! Interface to IOCP.

use super::Completion;

use blocking::Unblock;
use futures_lite::{future, prelude::*};

use std::cell::UnsafeCell;
use std::io::Result;
use std::marker::PhantomPinned;
use std::mem::{zeroed, ManuallyDrop, MaybeUninit};
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use windows_sys::Win32::Foundation as found;
use windows_sys::Win32::System::IO as wio;

const WAKEUP_KEY: u32 = std::u32::MAX;
const SHUTDOWN_KEY: u32 = std::u32::MAX - 1;

pub(crate) struct Ring {
    /// The raw handle to the IO completion port.
    ///
    /// Logically, this is owned by `EventLoop`.
    port: ManuallyDrop<CompletionPort>,

    /// Event loop governing events.
    event_loop: Unblock<EventLoop>,

    /// Event notifications for fixed events.
    notifications: Pin<Box<Notifications>>,
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
        let event_loop = Unblock::new(MAX_EVENTS, EventLoop::new(&port));

        Ok(Self {
            port,
            event_loop,
            notifications: Box::pin(Notifications {
                wakeup: UnsafeCell::new(unsafe { zeroed() }),
                shutdown: UnsafeCell::new(unsafe { zeroed() }),
            }),
        })
    }

    pub(crate) fn events(&self) -> io::Result<Events> {
        Ok(Events(None))
    }

    pub(crate) fn register(&self, fd: RawHandle) -> io::Result<()> {
        self.port.associate(fd as _, 0x1337)
    }

    pub(crate) fn deregister(&self, _fd: RawHandle) -> io::Result<()> {
        // Does nothing for now.
        Ok(())
    }

    pub(crate) unsafe fn submit(&self, operation: Pin<&mut Operation>) -> io::Result<()> {
        // Run with the event loop.
        let project = operation.project();
        todo!()
    }

    pub(crate) fn cancel(&self, operation: Pin<&mut Operation>, _key: u64) -> io::Result<()> {
        // Run CancelIoEx to cancel this specific operation.
        let mut operation = operation.project();
        let handle = operation.handle;
        let overlapped = &mut operation.overlapped;

        let result = unsafe { wio::CancelIoEx(handle, overlapped) };

        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn wait(&self, events: &mut Events) -> io::Result<usize> {
        // We are looking for the first non-empty event cache.
        let mut resolve_event = self.event_loop.find(|event| event.len != 0);

        // Poll it once to see if it's already ready.
        match future::poll_once(&mut resolve_event).await {
            Some(Some(events)) => {
                // One is already available, we're done.
                events.0 = Some(events);
                return Ok(events.len);
            }
            Some(None) => {
                // Notify the reactor and begin polling in ernest.
            }
            None => panic!("event loop terminated"),
        }

        // Notify the event loop so that it returns early.
        unsafe {
            self.port
                .notify(WAKEUP_KEY, self.notifications.wakeup.get())?;
        }

        // Wait for the event loop to return.
        match resolve_event.await {
            Some(events) => {
                events.0 = Some(events);
                return Ok(events.len);
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
    pub(crate) fn iter(&self) -> impl Iterator<Item = Completion> {
        self.0
            .iter()
            .flat_map(|inner| inner.entries())
            .filter_map(|entry| {
                if matches!(entry.lpCompletionKey, WAKEUP_KEY | SHUTDOWN_KEY) {
                    None
                } else {
                    todo!()
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
    actual: usize,
}

impl InnerEvents {
    fn new() -> Self {
        Self {
            buffer: Box::new(unsafe { MaybeUninit::uninit().assume_init() }),
            len: 0,
            actual: 0,
        }
    }

    fn open_space(&mut self) -> &mut [MaybeUninit<wio::OVERLAPPED_ENTRY>] {
        &mut self.buffer[self.len..]
    }

    /// Iterate over valid entries.
    fn entries(&self, tail: usize) -> impl Iterator<Item = &wio::OVERLAPPED_ENTRY> {
        let len = self.len - tail;
        self.buffer[tail..len]
            .iter()
            .map(|e| unsafe { &*e.as_ptr() })
    }
}

pin_project_lite::pin_project! {
    #[repr(C)]
    pub(crate) struct Operation {
        // The OVERLAPPED structure for operation data storage.
        overlapped: wio::OVERLAPPED,

        /// The raw handle to the IO completion port.
        handle: found::HANDLE,

        /// The type of this operation.
        ty: OpType,

        /// The key associated with this operation.
        key: u64,

        #[pin]
        _pin: PhantomPinned,
    }
}

#[derive(Copy, Clone)]
enum OpType {
    Read,
    Write,
}

impl OpType {
    /// Run this operation on the given handle.
    unsafe fn run(
        &self,
        handle: found::HANDLE,
        overlapped: *mut wio::OVERLAPPED,
        buffer: *mut u8,
        len: u32,
    ) -> io::Result<isize> {
        let result = match self {
            OpType::Read => wio::ReadFile(handle, buffer, len, overlapped),
            OpType::Write => wio::WriteFile(handle, buffer, len, overlapped),
        };

        if result == found::ERROR_IO_PENDING {
        } else if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result)
        }
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
        let handle = unsafe { wio::CreateIoCompletionPort(found::INVALID_HANDLE_VALUE, 0, 0, 0) };

        if handle == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }

    /// Associate a handle with this completion port.
    fn associate(&self, handle: found::HANDLE, key: u32) -> io::Result<()> {
        let handle = unsafe { wio::CreateIoCompletionPort(handle, self.0, key, 0) };

        if handle == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Wait for events from this `CompletionPort`.
    fn wait(&self, events: &mut InnerEvents) -> io::Result<usize> {
        // Stack space for results.
        let mut result_entries = MaybeUninit::uninit();
        let space = events.open_space();

        // Call the function.
        let res = unsafe {
            wio::GetQueuedCompletionStatusEx(
                self.0,
                space.as_mut_ptr() as _,
                space.len() as _,
                result_entries.as_mut_ptr(),
                found::INFINITE,
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
    unsafe fn notify(&self, key: u32, overlapped: *mut wio::OVERLAPPED) -> io::Result<()> {
        let res = unsafe { wio::PostQueuedCompletionStatus(self.0, 0, key, overlapped) };

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

            // Run over the events we've received and see if there are any signals.
            let mut notified = false;
            for entry in self.events.entries(new_events) {
                if entry.lpCompletionKey == WAKEUP_KEY {
                    // Don't notify if this is the only event.
                    if new_events == 1 {
                        notified = true;
                    }
                } else if entry.lpCompletionKey == SHUTDOWN_KEY {
                    self.shutdown = true;
                    return None;
                }
            }

            // If we're notified or if we have too many entries, return the buffer.
            if notified || self.events.len >= self.events.buffer.len() {
                let events = mem::replace(&mut self.events, InnerEvents::new());

                return Some(events);
            }

            // Continue waiting.
        }
    }
}

impl std::iter::FusedIterator for EventLoop {}
