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
//!
//! [`io_uring`]: https://kernel.dk/io_uring.pdf
//! [`IOCP`]: https://docs.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports
//! [`async-io`]: https://crates.io/crates/async-io

#![deny(rust_2018_idioms, future_incompatible)]

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::marker::PhantomPinned;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::NonNull;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use async_lock::Mutex;

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
    /// 
    /// A different handle may need to be used when actually polling
    /// for events, depending on the platform. This function returns
    /// that handle.
    pub fn register(&self, source: &impl AsRaw) -> io::Result<Raw> {
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
    pub unsafe fn submit<'a, B: AsyncParameter>(
        &'a self,
        operation: Pin<&mut Operation<'a, B>>,
    ) -> io::Result<()> {
        let mut project = operation.project();

        project.cancel.ring = Some(self);

        // Create the operation and add it to our pin.
        project.operation.set(Some(
            project
                .params
                .make_operation(&*project.buffer, project.cancel.key),
        ));

        // SAFETY: Operation can only contain valid events.
        self.inner.submit(project.operation.as_pin_mut().unwrap())
    }

    /// Cancels an operation.
    fn cancel<B>(&self, op: Pin<&mut Operation<'_, B>>) -> io::Result<()> {
        let project = op.project();

        self.inner
            .cancel(project.operation.as_pin_mut().unwrap(), project.cancel.key)
    }

    /// Wait for completion events.
    pub async fn wait(&self, events: &mut Vec<Completion>) -> io::Result<usize> {
        // If we can resolve any events early, do so.
        let early_count = self.inner.steal_early(events);

        // Lock the completion queue.
        if let Some(mut lock) = self.events.try_lock() {
            // Wait for events with the completion interface.
            let amt = self.inner.wait(&mut lock, early_count == 0).await?;

            // Extend user events with ours.
            events.extend(lock.iter());

            // Return th number of events.
            Ok(early_count.saturating_add(amt))
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

unsafe impl<T: Send> Send for Operation<'_, T> {}
unsafe impl<T: Sync> Sync for Operation<'_, T> {}

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
    unsafe fn make_operation<B: AsyncParameter>(&self, buffer: &B, key: u64) -> sys::Operation {
        match self {
            Self::Read { fd, len, offset } => {
                let ptr = buffer.ptr().unwrap_or(NonNull::dangling());
                sys::Operation::read(*fd, ptr.as_ptr(), *len, *offset, key)
            }
            Self::Write { fd, len, offset } => {
                let ptr = buffer.ptr().unwrap_or(NonNull::dangling());
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

/// An object that can be passed into an asynchronous operation.
///
/// This is a base trait that is implemented by `Buf`, `BufMut` and others.
///
/// # Safety
///
/// This safety requirements for this trait depend on the downstream traits implemented by the
/// implementor.
#[allow(clippy::len_without_is_empty)]
pub unsafe trait AsyncParameter {
    /// The pointer into the buffer, if any.
    fn ptr(&self) -> Option<NonNull<u8>>;

    /// The pointer into the second buffer, if any.
    fn ptr2(&self) -> Option<NonNull<u8>>;

    /// The length of the first buffer, if any.
    fn len(&self) -> usize;

    /// The length of the second buffer, if any.
    fn len2(&self) -> usize;
}

unsafe impl<const N: usize> AsyncParameter for [u8; N] {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        N
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl<const N: usize> AsyncParameter for &'static [u8; N] {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        N
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for &'static [u8] {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for &'static mut [u8] {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for &'static str {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.as_bytes().first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        <str>::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for Vec<u8> {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for Box<[u8]> {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.first().map(NonNull::from)
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for &'static OsStr {
    fn ptr(&self) -> Option<NonNull<u8>> {
        #[cfg(unix)]
        return self.as_bytes().first().map(NonNull::from);

        // SAFETY: just cast the pointer to a u8 pointer.
        #[cfg(windows)]
        return unsafe { Some(NonNull::new_unchecked(*self as *const _ as *mut u8)) };
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        OsStr::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for OsString {
    fn ptr(&self) -> Option<NonNull<u8>> {
        #[cfg(unix)]
        return self.as_bytes().first().map(NonNull::from);

        // SAFETY: just cast the pointer to a u8 pointer.
        #[cfg(windows)]
        return unsafe {
            Some(NonNull::new_unchecked(
                self.as_os_str() as *const OsStr as *const u8 as *mut u8,
            ))
        };
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        <OsStr>::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for Box<OsStr> {
    fn ptr(&self) -> Option<NonNull<u8>> {
        #[cfg(unix)]
        return self.as_bytes().first().map(NonNull::from);

        // SAFETY: just cast the pointer to a u8 pointer.
        #[cfg(windows)]
        return unsafe {
            Some(NonNull::new_unchecked(
                (&**self) as *const OsStr as *const u8 as *mut u8,
            ))
        };
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len(&self) -> usize {
        OsStr::len(self)
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for &'static Path {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.as_os_str().ptr()
    }

    fn len(&self) -> usize {
        self.as_os_str().len()
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for PathBuf {
    fn ptr(&self) -> Option<NonNull<u8>> {
        #[cfg(unix)]
        return self.as_os_str().as_bytes().first().map(NonNull::from);

        // SAFETY: just cast the pointer to a u8 pointer.
        #[cfg(windows)]
        return unsafe {
            Some(NonNull::new_unchecked(
                self.as_os_str() as *const OsStr as *const u8 as *mut u8,
            ))
        };
    }

    fn len(&self) -> usize {
        self.as_os_str().len()
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl AsyncParameter for Box<Path> {
    fn ptr(&self) -> Option<NonNull<u8>> {
        #[cfg(unix)]
        return self.as_os_str().as_bytes().first().map(NonNull::from);

        // SAFETY: just cast the pointer to a u8 pointer.
        #[cfg(windows)]
        return unsafe {
            Some(NonNull::new_unchecked(
                self.as_os_str() as *const OsStr as *const u8 as *mut u8,
            ))
        };
    }

    fn len(&self) -> usize {
        self.as_os_str().len()
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        None
    }

    fn len2(&self) -> usize {
        0
    }
}
unsafe impl<A: AsyncParameter, B: AsyncParameter> AsyncParameter for (A, B) {
    fn ptr(&self) -> Option<NonNull<u8>> {
        self.0.ptr()
    }

    fn ptr2(&self) -> Option<NonNull<u8>> {
        self.1.ptr()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn len2(&self) -> usize {
        self.1.len()
    }
}

/// A buffer that may be passed to a system-specific operation.
///
/// This is implied to be some kind of memory buffer that may be used by the
/// system to store data.
///
/// # Safety
///
/// - `ptr()` must be a pointer to the first element of the buffer, or
///   `None` if the buffer is empty.
/// - `len()` must be the number of elements the buffer is valid for.
/// - `ptr2()` must be `None`.
/// - `len2()` must be `0`.
pub unsafe trait Buf: AsyncParameter {}

unsafe impl<const N: usize> Buf for [u8; N] {}
unsafe impl<const N: usize> Buf for &'static [u8; N] {}
unsafe impl Buf for &'static [u8] {}
unsafe impl Buf for &'static mut [u8] {}
unsafe impl Buf for &'static str {}
unsafe impl Buf for Vec<u8> {}
unsafe impl Buf for Box<[u8]> {}

/// A `Buf` whose data can also be mutably accessed.
///
/// # Safety
///
/// Safe as [`Buf`], but `ptr()` must also be valid for mutable writes.
pub unsafe trait BufMut: Buf {}

unsafe impl<const N: usize> BufMut for [u8; N] {}
unsafe impl BufMut for &'static mut [u8] {}
unsafe impl BufMut for Vec<u8> {}
unsafe impl BufMut for Box<[u8]> {}

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
