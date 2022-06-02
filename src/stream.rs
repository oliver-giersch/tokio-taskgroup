use std::{
    cmp, fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task;
use tokio_stream::{Stream, StreamExt};

use crate::TaskName;

type TaskJoinResult<E> = Result<(), (TaskName, TaskError<E>)>;

#[derive(Debug)]
pub enum TaskError<E> {
    Panic,
    Error(E),
}

impl<E: cmp::PartialEq> cmp::PartialEq for TaskError<E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Error(e0), Self::Error(e1)) => e0.eq(e1),
            (Self::Panic, Self::Panic) => false, // two panics are never equal
            _ => false,
        }
    }
}

impl<E: cmp::Eq> cmp::Eq for TaskError<E> {}

impl<E: fmt::Display> fmt::Display for TaskError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

/// A stream of task join handles which produces an item whenever a task handle
/// is joined.
pub(crate) struct JoinHandleStream<E = ()> {
    handles: Vec<(TaskName, task::JoinHandle<Result<(), E>>)>,
    closed: bool,
}

impl<E> JoinHandleStream<E> {
    /// Returns a new stream with an initial `capacity`.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self { handles: Vec::with_capacity(capacity), closed: false }
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
    }

    pub(crate) fn insert(&mut self, name: TaskName, handle: task::JoinHandle<Result<(), E>>) {
        assert!(!self.closed, "can not insert into closed stream");
        self.handles.push((name, handle));
    }

    /// Closes the stream, aborts all currently present tasks and waits for the
    /// cancellation to complete.
    pub(crate) async fn abort_all(&mut self) {
        self.closed = true;
        for (_, handle) in &self.handles {
            handle.abort();
        }

        while let Some(_) = self.next().await {}
    }

    fn poll_next_join(&mut self, cx: &mut Context<'_>) -> Poll<Option<TaskJoinResult<E>>> {
        // poll all handles
        for i in 0..self.handles.len() {
            let (_, handle) = &mut self.handles[i];
            if let Poll::Ready(res) = Pin::new(handle).poll(cx) {
                let (name, _) = self.handles.swap_remove(i);

                let res = match res {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err((name, TaskError::Error(e))),
                    Err(join) => match join.try_into_panic() {
                        Ok(_) => Err((name, TaskError::Panic)),
                        Err(_) => Ok(()),
                    },
                };

                return Poll::Ready(Some(res));
            }
        }

        if self.closed && self.handles.is_empty() {
            return Poll::Ready(None);
        }

        self.shrink_if_oversized();
        Poll::Pending
    }

    fn shrink_if_oversized(&mut self) {
        if self.handles.len() * 4 < self.handles.capacity() {
            self.handles.shrink_to_fit();
        }
    }
}

impl<E> Stream for JoinHandleStream<E> {
    type Item = TaskJoinResult<E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_join(cx)
    }
}

#[cfg(test)]
mod tests {

    use tokio::task;
    use tokio_stream::StreamExt;

    use crate::stream::TaskError;

    use super::JoinHandleStream;

    #[test]
    fn close() {
        crate::block_on(async {
            let mut stream = JoinHandleStream::<()>::with_capacity(2);

            stream.insert("a".into(), task::spawn(async { Ok(()) }));
            stream.insert("b".into(), task::spawn(async { Ok(()) }));

            stream.close();

            let next = stream.next().await;
            assert_eq!(next, Some(Ok(())));
            let next = stream.next().await;
            assert_eq!(next, Some(Ok(())));
            let next = stream.next().await;
            assert!(next.is_none());
        });
    }

    #[test]
    fn abort() {
        crate::block_on(async {
            let mut stream = JoinHandleStream::<()>::with_capacity(2);

            stream.insert("a".into(), task::spawn(async { Ok(()) }));
            stream.insert("b".into(), task::spawn(std::future::pending()));

            stream.abort_all().await;

            let next = stream.next().await;
            assert!(next.is_none());
        })
    }

    #[test]
    fn error() {
        crate::block_on(async {
            let mut stream = JoinHandleStream::<i32>::with_capacity(2);

            stream.insert("a".into(), task::spawn(async { Ok(()) }));
            stream.insert("b".into(), task::spawn(async { Err(-1) }));

            let r0 = stream.next().await.unwrap();
            let r1 = stream.next().await.unwrap();

            let (name, err) = match (r0, r1) {
                (Ok(_), Err(e)) | (Err(e), Ok(_)) => e,
                _ => panic!("expected exactly one task failure"),
            };

            assert_eq!(name, "b");
            assert_eq!(err, TaskError::Error(-1));
        });
    }

    #[test]
    fn panic() {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        crate::block_on(async {
            let mut stream = JoinHandleStream::<i32>::with_capacity(2);

            stream.insert("a".into(), task::spawn(async { Ok(()) }));
            stream.insert("b".into(), task::spawn(async { panic!("fatal") }));

            let r0 = stream.next().await.unwrap();
            let r1 = stream.next().await.unwrap();

            let (name, err) = match (r0, r1) {
                (Ok(_), Err(e)) | (Err(e), Ok(_)) => e,
                _ => panic!("expected exactly one task failure"),
            };

            assert_eq!(name, "b");
            assert_eq!(err, TaskError::Panic);
        });

        std::panic::set_hook(hook);
    }
}
