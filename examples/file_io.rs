//! Brief example of cross-platform file I/O.

use submission::{Operation, Raw, Ring};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    async_io::block_on(async {
        // Create a new ring.
        let ring = Ring::new()?;

        // Open a file.
        // There is not a cross-platform way to open a file, so just use the thread pool.
        let mut file = blocking::unblock(|| std::fs::File::create("foo.txt")).await?;

        println!("Writing to file...");

        let handle = Wrapper(ring.register(&file)?);

        // Create an operation to write to the file.
        let write_op =
            unsafe { Operation::<'_, ()>::with_key(0).write(&handle, b"Hello, world!", 0) };

        // Submit the operation to the ring.
        pin_utils::pin_mut!(write_op);
        unsafe {
            ring.submit(write_op.as_mut())?;
        }

        // Wait for complete events.
        let mut completion = 'poll: loop {
            let mut events = vec![];
            ring.wait(&mut events).await?;

            for event in events {
                if event.key() == 0 {
                    break 'poll event;
                }
            }
        };

        // Get the result of the operation.
        completion.result()?;

        // Close the file and reopen it.
        drop(file);
        file = blocking::unblock(|| std::fs::File::open("foo.txt")).await?;

        println!("Reading from file...");

        let handle = Wrapper(ring.register(&file)?);

        // Create an operation to read from the file.
        let read_op = unsafe { Operation::<'_, ()>::with_key(1).read(&handle, vec![0; 13], 0) };

        // Submit the operation to the ring.
        pin_utils::pin_mut!(read_op);
        unsafe {
            ring.submit(read_op.as_mut())?;
        }

        // Wait for complete events.
        let mut completion = 'poll: loop {
            let mut events = vec![];
            ring.wait(&mut events).await?;

            for event in events {
                if event.key() == 1 {
                    break 'poll event;
                }
            }
        };

        // Get the result of the operation.
        completion.result()?;
        let buffer = std::mem::take(read_op.unlock(&completion).unwrap().buffer_mut());

        // Print the contents of the file.
        println!("File contents: {}", String::from_utf8_lossy(&buffer));

        // Delete the file to clean it up.
        drop(file);
        blocking::unblock(|| std::fs::remove_file("foo.txt")).await?;

        Ok(())
    })
}

struct Wrapper(Raw);

#[cfg(unix)]
impl std::os::unix::io::AsRawFd for Wrapper {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.0
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawHandle for Wrapper {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        self.0
    }
}
