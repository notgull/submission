use async_io::block_on;
use criterion::{criterion_group, criterion_main, Criterion};
use std::io::prelude::*;
use std::{fs, mem};
use submission::{Operation, Ring};

fn submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("submit");
    let ring = Ring::new().unwrap();

    group.bench_function("Read", |b| {
        let mut file = fs::File::create("foo.txt").unwrap();
        let buf = vec![0u8; 10_000];
        file.write_all(&buf).unwrap();
        let mut buf = Some(buf);

        b.iter(|| {
            block_on(async {
                let buf2 = buf.take().unwrap();
                let read_op = unsafe { Operation::<'_, ()>::with_key(0).read(&file, buf2, 0) };

                pin_utils::pin_mut!(read_op);

                unsafe {
                    ring.submit(read_op.as_mut()).ok();
                }

                loop {
                    let mut events = vec![];
                    ring.wait(&mut events).await.ok();

                    for event in events {
                        if event.key() == 0 {
                            let buf2 = mem::take(read_op.unlock(&event).unwrap().buffer_mut());
                            buf = Some(buf2);
                            return;
                        }
                    }
                }
            })
        });

        fs::remove_file("foo.txt").unwrap();
    });

    group.bench_function("Write", |b| {
        let file = fs::File::create("foo.txt").unwrap();
        let mut buf = Some(vec![0u8; 10_000]);

        b.iter(|| {
            block_on(async {
                let buf2 = buf.take().unwrap();
                let write_op = unsafe { Operation::<'_, ()>::with_key(0).write(&file, buf2, 0) };

                pin_utils::pin_mut!(write_op);

                unsafe {
                    ring.submit(write_op.as_mut()).ok();
                }

                loop {
                    let mut events = vec![];
                    ring.wait(&mut events).await.ok();

                    for event in events {
                        if event.key() == 0 {
                            let buf2 = mem::take(write_op.unlock(&event).unwrap().buffer_mut());
                            buf = Some(buf2);
                            return;
                        }
                    }
                }
            })
        });

        fs::remove_file("foo.txt").unwrap();
    });
}

criterion_group! {
    submit_benches,
    submit,
}

criterion_main!(submit_benches);
