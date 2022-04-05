use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task;
use tokio_stream::{Stream, StreamExt};

use crate::TaskName;

type TaskJoinResult<E> = Result<Result<(), E>, task::JoinError>;

pub(crate) struct JoinHandleStream<E> {
    handles: Vec<(TaskName, task::JoinHandle<Result<(), E>>)>,
    closed: bool,
}

impl<E> JoinHandleStream<E> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            handles: Vec::with_capacity(capacity),
            closed: false,
        }
    }

    pub(crate) fn insert(&mut self, name: TaskName, handle: task::JoinHandle<Result<(), E>>) {
        debug_assert!(!self.closed);
        self.handles.push((name, handle));
    }

    pub(crate) async fn abort_all(&mut self) {
        self.closed = true;
        for (_, handle) in &self.handles {
            handle.abort();
        }

        while let Some(_) = self.next().await {}
    }

    fn poll_next_join(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(TaskName, TaskJoinResult<E>)>> {
        for i in 0..self.handles.len() {
            let (_, handle) = &mut self.handles[i];
            if let Poll::Ready(res) = Pin::new(handle).poll(cx) {
                let (name, _) = self.handles.swap_remove(i);
                self.shrink_if_oversized();

                return Poll::Ready(Some((name, res)));
            }
        }

        if self.closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn shrink_if_oversized(&mut self) {
        if self.handles.len() * 4 < self.handles.capacity() {
            self.handles.shrink_to_fit();
        }
    }
}

impl<E> Stream for JoinHandleStream<E> {
    type Item = (TaskName, TaskJoinResult<E>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_join(cx)
    }
}
