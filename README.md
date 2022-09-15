# submission

The submission crate is a portable API over the completion-based API system for several systems. As of now, two systems are supported:

- Later versions of Linux, using the [`io_uring`] interface.
- Windows, using the [`I/O Completion Ports`] interface.

[`io_uring`]: https://kernel.dk/io_uring.pdf
[`I/O Completion Ports`]: https://docs.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports

For operating systems where this system does not exist, such as macOS or earlier versions of Linux, these APIs just return an error.

## License

This project is licensed under either of the following, at your option:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)