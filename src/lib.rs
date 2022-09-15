//! Interface to the system's I/O completion-based asynchronous API.
//!
//! This crate acts as a safe wrapper around the system's I/O completion-based
//! asynchronous API, which is exposed on the following operating systems:
//!
//! - Linux 5.1+, through [`io_uring`]. The [`async-io`] crate is used to provide
//!   thread-free waiting on I/O events.
//! - Windows, through [`IOCP`]. On first usage, a thread is spawned that constantly
//!   polls for new I/O events.
//!
//! Due to system limitations, this crate only supports `'static` buffers, pinned in
//! place to prevent them from moving while the I/O operation is in flight.
//!
//! # Usage
//!
//! Usage occurs through the [`Ring`] type, which represents the completion instance.

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
    } else if #[cfg(windows)] {
        mod iocp;
        use iocp as sys;
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
        let events = Mutex::new(inner.events()?);

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
    ///
    /// # Safety
    ///
    /// Between the time that this function is called and the time that the
    /// operation completes, the buffer must not be moved or forgotten.
    pub unsafe fn submit<'a, B: Buf>(
        &'a self,
        operation: Pin<&mut Operation<'a, B>>,
    ) -> io::Result<()> {
        let mut project = operation.project();

        project.cancel.ring = Some(self);

        // Create the operation and add it to our pin.
        project.operation.set(Some(unsafe {
            project
                .params
                .make_operation(&*project.buffer, project.cancel.key)
        }));

        // SAFETY: Operation can only contain valid events.
        unsafe { self.inner.submit(project.operation.as_pin_mut().unwrap()) }
    }

    /// Cancels an operation.
    fn cancel<B>(&self, op: Pin<&mut Operation<'_, B>>) -> io::Result<()> {
        let project = op.project();

        self.inner
            .cancel(project.operation.as_pin_mut().unwrap(), project.cancel.key)
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

/// An operation to be submitted to the ring.
///
/// This is a wrapper around the system-specific operation type as well as
/// the user buffer that may be associated with it.
// Note: Neither pin_project nor pin_project_lite's semantics allow for what we
// want to do with this, so we manually project.
pub struct Operation<'a, T> {
    // The wrapper around the system-specific operation type.
    //
    // This is pinned; however, `sys::Operation` may be `Unpin`. It is set once the
    // operation is submitted to the ring.
    operation: Option<sys::Operation>,

    // Details on how to begin the operation.
    params: OperationParams,

    // The user buffer that may be associated with the operation.
    //
    // This is considered to be pinned.
    buffer: T,

    // Data that can be used to cancel this operation.
    cancel: Canceller<'a>,

    // Prevents `Operation` from being `Unpin`.
    _pin: PhantomPinned,
}

/// Pin-projection of `Operation`.
///
/// Neither `pin_project` nor `pin_project_lite`'s semantics allow for what we want to do with this,
/// so we manually project.
struct OperationProj<'r, 'a, T> {
    operation: Pin<&'r mut Option<sys::Operation>>,
    params: &'r mut OperationParams,
    cancel: &'r mut Canceller<'a>,
    buffer: Pin<&'r mut T>,
}

impl<'a, T> Drop for Operation<'a, T> {
    fn drop(&mut self) {
        // SAFETY: `self` is pinned but `ring` is not.
        let ring = self.cancel.ring;

        // If the operation was submitted, cancel it.
        if let Some(ring) = ring {
            let _ = ring.cancel(unsafe { Pin::new_unchecked(self) });
        }
    }
}

impl<'a, T> fmt::Debug for Operation<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Operation { .. }")
    }
}

impl<'a, T> Operation<'a, T> {
    fn project<'proj>(self: Pin<&'proj mut Self>) -> OperationProj<'proj, 'a, T> {
        unsafe {
            let Self {
                operation,
                params,
                buffer,
                cancel,
                ..
            } = self.get_unchecked_mut();
            OperationProj {
                operation: Pin::new_unchecked(operation),
                params,
                cancel,
                buffer: Pin::new_unchecked(buffer),
            }
        }
    }

    /// Get the key for this operation.
    pub fn key(&self) -> u64 {
        self.cancel.key
    }

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
    pub fn cancel(mut self: Pin<&mut Self>) -> io::Result<&mut Self> {
        // If we have a ring, cancel ourselves on it.
        if let Some(ring) = self.cancel.ring {
            ring.cancel(self.as_mut())?;

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
        Operation {
            operation: None,
            params: OperationParams::Read {
                fd: source.as_raw(),
                len: buffer.len(),
                offset,
            },
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
        Operation {
            operation: None,
            params: OperationParams::Write {
                fd: source.as_raw(),
                len: buffer.len(),
                offset,
            },
            buffer,
            cancel: Canceller::new(self.key),
            _pin: PhantomPinned,
        }
    }
}

/// Parameters used to build an `sys::Operation`.
enum OperationParams {
    /// Read the specified number of bytes from the specified file descriptor.
    Read { fd: Raw, len: usize, offset: u64 },

    /// Write the specified number of bytes to the specified file descriptor.
    Write { fd: Raw, len: usize, offset: u64 },
}

impl OperationParams {
    unsafe fn make_operation<B: Buf>(&self, buffer: &B, key: u64) -> sys::Operation {
        match self {
            Self::Read { fd, len, offset } => {
                let ptr = buffer.ptr().unwrap();
                sys::Operation::read(*fd, ptr.as_ptr(), *len, *offset, key)
            }
            Self::Write { fd, len, offset } => {
                let ptr = buffer.ptr().unwrap();
                sys::Operation::write(*fd, ptr.as_ptr(), *len, *offset, key)
            }
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

/// A completion-based asynchronous operation that has completed.
#[derive(Debug)]
pub struct Completion {
    /// The key for this operation.
    key: u64,

    /// The result of this operation.
    result: Option<io::Result<isize>>,
}

impl Completion {
    /// Get the user data associated with the operation.
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Get the result of the operation.
    pub fn result(&mut self) -> io::Result<isize> {
        self.result.take().expect("result already taken")
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

        pub type Raw = RawFd;

        pub trait AsRaw : AsRawFd {
            fn as_raw(&self) -> Raw {
                self.as_raw_fd()
            }
        }

        impl<T: AsRawFd> AsRaw for T {}
    } else if #[cfg(windows)] {
        use std::os::windows::io::{AsRawHandle, RawHandle};

        pub type Raw = RawHandle;

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
