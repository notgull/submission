//! An API for when a completion API is not available.

use super::{Completion, Raw};

use std::fmt;
use std::io;
use std::pin::Pin;

/// None of these objects should ever actually exist.
enum Uninhabited {}

pub(crate) struct Ring(Uninhabited);
pub(crate) struct Events(Uninhabited);
pub(crate) struct Operation;

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {}
    }
}

impl Ring {
    pub(crate) fn new() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "completion API not available",
        ))
    }

    pub(crate) fn events(&self) -> io::Result<Events> {
        match self.0 {}
    }

    pub(crate) fn register(&self, _source: Raw) -> io::Result<()> {
        match self.0 {}
    }

    pub(crate) fn deregister(&self, _source: Raw) -> io::Result<()> {
        match self.0 {}
    }

    pub(crate) unsafe fn submit(&self, _operation: Pin<&mut Operation>) -> io::Result<()> {
        match self.0 {}
    }

    pub(crate) fn cancel(&self, key: u64) -> io::Result<()> {
        match self.0 {}
    }

    pub(crate) async fn wait(&self, _events: &mut Events) -> io::Result<usize> {
        match self.0 {}
    }
}

impl Events {
    pub(crate) fn new() -> Self {
        panic!("completion API not available")
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Completion> {
        match self.0 {}

        std::iter::empty()
    }
}

impl Operation {
    pub(crate) unsafe fn read(fd: Raw, buf: *mut u8, len: usize) -> Self {
        Self
    }

    pub(crate) unsafe fn write(fd: Raw, buf: *const u8, len: usize) -> Self {
        Self
    }
}
