//! Interface to the system's I/O completion-based asynchronous API.

#![deny(rust_2018_idioms, future_incompatible)]

use std::fmt;
use std::io;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Mutex;

use __private::Sealed;

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        mod uring;
        use uring as sys;
    } else {
        mod unavailable;
        use unavailable as sys;
    }
}

/// A portable interface to the systems' completion-based asynchronous API.
pub struct Ring {
    /// The inner completion interface.
    inner: sys::Ring,

    /// A buffer used to hold the list of completed events.
    events: Mutex<sys::Events>,
}

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl Ring {
    /// Initializes a new `Ring`.
    ///
    /// This function will initialize a new completion interface and allocate
    /// the necessary resources.
    pub fn new() -> io::Result<Self> {
        let inner = sys::Ring::new()?;
        let events = Mutex::new(sys::Events::new());

        Ok(Self { inner, events })
    }

    /// Register a source of events.
    pub fn register(&self, source: &impl AsRaw) -> io::Result<()> {
        self.inner.register(source.as_raw())
    }

    /// Deregister a source of events.
    pub fn deregister(&self, source: &impl AsRaw) -> io::Result<()> {
        self.inner.deregister(source.as_raw())
    }

    /// Submits an operation to the completion interface.
    pub fn submit<'a, B>(&'a self, operation: Pin<&mut Operation<'a, B>>) -> io::Result<()> {
        let project = operation.project();
        project.cancel.ring = Some(self);

        // SAFETY: Operation can only contain valid events.
        unsafe { self.inner.submit(project.operation) }
    }

    /// Cancels an operation.
    fn cancel(&self, key: u64) -> io::Result<()> {
        self.inner.cancel(key)
    }

    /// Wait for completion events.
    #[allow(clippy::await_holding_lock)]
    pub async fn wait(&self, events: &mut Vec<Completion>) -> io::Result<usize> {
        // Lock the completion queue.
        if let Ok(mut lock) = self.events.try_lock() {
            // Wait for events with the completion interface.
            let amt = self.inner.wait(&mut lock).await?;

            // Extend user events with ours.
            events.extend(lock.iter());

            // Return th number of events.
            Ok(amt)
        } else {
            Ok(0)
        }
    }
}

pin_project_lite::pin_project! {
    /// An operation to be submitted to the ring.
    ///
    /// This is a wrapper around the system-specific operation type as well as
    /// the user buffer that may be associated with it.
    pub struct Operation<'a, T> {
        // The wrapper around the system-specific operation type.
        //
        // This is pinned; however, `sys::Operation` may be `Unpin`.
        #[pin]
        operation: sys::Operation,

        // The user buffer that may be associated with the operation.
        //
        // This is considered to be pinned.
        #[pin]
        buffer: T,

        // Data that can be used to cancel this operation.
        cancel: Canceller<'a>,

        // Prevents `Operation` from being `Unpin`.
        #[pin]
        _pin: PhantomPinned,
    }
}

impl<'a, T> fmt::Debug for Operation<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Operation { .. }")
    }
}

impl<'a, T> Operation<'a, T> {
    /// Get a reference to the inner buffer.
    ///
    /// This is safe, since the fact that the `Operation` is not yet pinned indicates that
    /// the buffer is not yet in use.
    pub fn buffer(&self) -> &T {
        &self.buffer
    }

    /// Get a mutable reference to the inner buffer.
    ///
    /// This is safe, since the fact that the `Operation` is not yet pinned indicates that
    /// the buffer is not yet in use.
    pub fn buffer_mut(&mut self) -> &mut T {
        &mut self.buffer
    }

    /// Destroy this operation and return the inner buffer.
    ///
    /// This is safe, since the fact that the `Operation` is not yet pinned indicates that
    /// the buffer is not yet in use.
    pub fn into_buffer(self) -> T {
        self.buffer
    }

    /// "Unlock" an operation, allowing the buffer to be accessed.
    pub fn unlock(self: Pin<&mut Self>, complete: &Completion) -> Option<&mut Self> {
        if self.cancel.key == complete.key() {
            let this = unsafe { self.get_unchecked_mut() };
            this.cancel.ring = None;
            Some(this)
        } else {
            None
        }
    }

    /// Cancels this operation and returns the inner buffer.
    pub fn cancel(self: Pin<&mut Self>) -> io::Result<&mut Self> {
        // If we have a ring, cancel ourselves on it.
        if let Some(ring) = self.cancel.ring {
            ring.cancel(self.cancel.key)?;

            // Now we can unlock ourselves.
            let this = unsafe { self.get_unchecked_mut() };
            this.cancel.ring = None;
            Ok(this)
        } else {
            // We're not registered with a ring, so we can't cancel ourselves.
            // Just return the buffer.
            Ok(unsafe { self.get_unchecked_mut() })
        }
    }

    /// Build a new operation with this key.
    ///
    /// # Safety
    ///
    /// This key must be unique.
    pub unsafe fn with_key(key: u64) -> OperationBuilder {
        OperationBuilder { key }
    }
}

/// A structure for building an `Operation`.
#[derive(Debug)]
pub struct OperationBuilder {
    // Invariant: Key is unique.
    key: u64,
}

impl OperationBuilder {
    /// Create a new operation for reading from the specified file descriptor.
    pub fn read<'a, T: BufMut>(
        self,
        source: &impl AsRaw,
        buffer: T,
        offset: u64,
    ) -> Operation<'a, T> {
        // Create a new operation.
        // SAFETY: The 'BufMut' trait certifies its ability to be used here.
        let ptr = buffer.ptr().unwrap();
        let len = buffer.len();

        let op = unsafe { sys::Operation::read(source.as_raw(), ptr.as_ptr(), len, offset as _) };

        Operation {
            operation: op,
            buffer,
            cancel: Canceller::new(self.key),
            _pin: PhantomPinned,
        }
    }

    /// Create a new operation for writing to the specified file descriptor.
    pub fn write<'a, T: Buf>(
        self,
        source: &impl AsRaw,
        buffer: T,
        offset: u64,
    ) -> Operation<'a, T> {
        // Create a new operation.
        // SAFETY: The 'Buf' trait certifies its ability to be used here.
        let ptr = buffer.ptr().unwrap();
        let len = buffer.len();

        let op = unsafe { sys::Operation::write(source.as_raw(), ptr.as_ptr(), len, offset as _) };

        Operation {
            operation: op,
            buffer,
            cancel: Canceller::new(self.key),
            _pin: PhantomPinned,
        }
    }
}

struct Canceller<'a> {
    /// Our user's key.
    key: u64,

    /// The ring that owns this operation.
    ring: Option<&'a Ring>,
}

impl<'a> Canceller<'a> {
    pub fn new(key: u64) -> Self {
        Self { key, ring: None }
    }
}

impl<'a> Drop for Canceller<'a> {
    fn drop(&mut self) {
        // If we have a ring, cancel ourselves on it.
        if let Some(ring) = self.ring {
            ring.cancel(self.key).ok();
        }
    }
}

/// A completion-based asynchronous operation that has completed.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Completion {
    key: u64,
}

impl Completion {
    /// Get the user data associated with the operation.
    pub fn key(&self) -> u64 {
        self.key
    }
}

/// A buffer that may be passed to a system-specific operation.
///
/// This is implied to be some kind of memory buffer that may be used by the
/// system to store data.
///
/// # Safety
///
/// This trait is not meant to be implemented outside of this crate.
pub unsafe trait Buf: Sealed {}

unsafe impl<T: 'static + AsRef<[u8]>> Buf for T {}

/// A `Buf` whose data can also be mutably accessed.
///
/// # Safety
///
/// This trait is not meant to be implemented outside of this crate.
pub unsafe trait BufMut: Buf {}

unsafe impl<T: Buf + AsMut<[u8]>> BufMut for T {}

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        use std::os::unix::io::{AsRawFd, RawFd};

        type Raw = RawFd;

        pub trait AsRaw : AsRawFd {
            fn as_raw(&self) -> Raw {
                self.as_raw_fd()
            }
        }

        impl<T: AsRawFd> AsRaw for T {}
    } else if #[cfg(windows)] {
        use std::os::windows::io::{AsRawHandle, RawHandle};

        type Raw = RawHandle;

        pub trait AsRaw : AsRawHandle {
            fn as_raw(&self) -> Raw {
                self.as_raw_handle()
            }
        }

        impl<T: AsRawHandle> AsRaw for T {}
    } else {
        use std::convert::Infallible;

        type Raw = Infallible;

        pub trait AsRaw {
            fn as_raw(&self) -> Raw;
        }
    }
}

mod __private {
    use std::ptr::NonNull;

    #[doc(hidden)]
    pub trait Sealed {
        /// The pointer into the buffer, if any.
        fn ptr(&self) -> Option<NonNull<u8>>;

        /// The length of the buffer, if any.
        fn len(&self) -> usize;
    }

    impl<T: 'static + AsRef<[u8]>> Sealed for T {
        fn ptr(&self) -> Option<NonNull<u8>> {
            let slice = self.as_ref();
            NonNull::new(slice.as_ptr() as *mut u8)
        }

        fn len(&self) -> usize {
            self.as_ref().len()
        }
    }
}
