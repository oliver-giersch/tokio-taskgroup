mod stream;

use std::{borrow::Cow, future::Future};

use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tokio_stream::StreamExt;

use crate::stream::JoinHandleStream;

type TaskName = Cow<'static, String>;

// TODO: handle sender
// TODO: error receiver (mut)
pub struct TaskGroup<E> {
    tx: mpsc::UnboundedSender<Event<E>>,
    err_rx: (),
}

impl<E> TaskGroup<E> {
    pub fn new() -> Self {
        todo!()
    }

    pub fn close(self) -> Result<(), ()> {
        // send close message
        todo!()
    }

    // TODO: can fail, even if not close
    pub fn spawn<T>(name: impl Into<TaskName>, future: T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        // spawn
        task::spawn(future);
    }
}

pub struct SpawnErr;

enum Event<E> {
    Handle(TaskName, task::JoinHandle<Result<(), E>>),
    Closed,
}

async fn group_manager<E>(
    capacity: usize,
    tx: oneshot::Sender<(TaskName, Result<Result<(), E>, task::JoinError>)>,
    mut rx: mpsc::UnboundedReceiver<Event<E>>,
) {
    let mut stream = JoinHandleStream::<E>::with_capacity(capacity);
    let mut closed = false;

    let (task_name, err) = loop {
        let next = stream.next();
        tokio::pin!(next);

        tokio::select! {
            biased;
            res = &mut next => match res {
                Some((name, res)) => if let Err(e) = res {
                    stream.abort_all().await;
                    rx.close();
                    break (name, e);
                },
                // the task group is closed and all tasks have finished
                None => {
                    todo!("close")
                },
            },
            msg = rx.recv() => match msg {
                Some(Event::Handle(name, handle)) => {
                    assert!(!closed);
                    stream.insert(name, handle);
                },
                Some(Event::Closed) => {
                    closed = true;
                    stream.close();
                },
                None => {
                    // if the receiver is just dropped, cancel all tasks
                    stream.abort_all().await;
                    return;
                },
            }
        }
    };

    // todo: cancel all handles still in rx
}
