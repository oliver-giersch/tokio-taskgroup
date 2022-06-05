use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task;
use tokio_stream::{Stream, StreamExt};

use crate::{PanicPayload, TaskError, TaskName};

// if `Ok` the task finished successfully or was cancelled by the group manager,
// if `Err`, the task identified by its name encountered an error.
type TaskJoinResult<E> = Result<(), TaskError<E>>;

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

    /// Closes the stream, which lets the stream return `None` once all inserted
    /// join handles have been joined (otherwise it will always remain pending).
    pub(crate) fn close(&mut self) {
        assert!(self.closed == false, "can not close stream twice");
        self.closed = true;
    }

    /// Inserts a task `handle` with the given `name`.
    ///
    /// The name is not required to be unique.
    pub(crate) fn insert(&mut self, name: TaskName, handle: task::JoinHandle<Result<(), E>>) {
        assert!(self.closed == false, "can not insert into closed stream");
        self.handles.push((name, handle));
    }

    /// Closes the stream, aborts all currently present tasks and waits for the
    /// cancellation to complete.
    pub(crate) async fn abort_all(&mut self) {
        self.closed = true;
        for (_, handle) in &self.handles {
            handle.abort();
        }

        // `closed` must be set to true for this to work (and not deadlock)!
        while let Some(_) = self.next().await {}
    }

    /// Polls the next join result
    fn poll_next_join(&mut self, cx: &mut Context<'_>) -> Poll<Option<TaskJoinResult<E>>> {
        // poll all handles in sequence
        for i in 0..self.handles.len() {
            let (_, handle) = &mut self.handles[i];
            if let Poll::Ready(res) = Pin::new(handle).poll(cx) {
                // remove the handle from the vec
                let (name, _) = self.handles.swap_remove(i);
                let res = match res {
                    Ok(res) => res.or_else(|err| Err(TaskError::error(name, err))),
                    Err(join) => match join.try_into_panic() {
                        Ok(payload) => Err(TaskError::Panic(name, PanicPayload(payload))),
                        Err(_) => Ok(()), // task was cancelled (by group)
                    },
                };

                return Poll::Ready(Some(res));
            }
        }

        // the stream is done if the group is closed and there no more handles
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

            let err = match (r0, r1) {
                (Ok(_), Err(e)) | (Err(e), Ok(_)) => e,
                _ => panic!("expected exactly one task failure"),
            };

            assert_eq!(err.task_name(), "b");
            assert!(matches!(err, TaskError::Error(_, -1)));
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

            let err = match (r0, r1) {
                (Ok(_), Err(e)) | (Err(e), Ok(_)) => e,
                _ => panic!("expected exactly one task failure"),
            };

            assert_eq!(err.task_name(), "b");
            assert!(matches!(err, TaskError::Panic(_, _)));
        });

        std::panic::set_hook(hook);
    }
}
