mod stream;

use std::{borrow::Cow, future::Future};

use tokio::{sync::mpsc, task};
use tokio_stream::StreamExt;

use crate::stream::JoinHandleStream;

type TaskName = Cow<'static, String>;

// TODO: handle sender
// TODO: error receiver (mut)
pub struct TaskGroup;

impl TaskGroup {
    pub fn new() -> Self {
        todo!()
    }

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

async fn group_manager<E>(capacity: usize, mut rx: mpsc::UnboundedReceiver<Event<E>>) {
    let mut stream = JoinHandleStream::<E>::with_capacity(capacity);
    let mut closed = false;

    loop {
        let next = stream.next();
        tokio::pin!(next);

        tokio::select! {
            biased;
            res = &mut next => match res {
                Some((name, res)) => match res {
                    Ok(_) => todo!("nothing?"),
                    Err(e) => todo!("abort all tasks")
                },
                None => todo!("implies closed"),
            },
            msg = rx.recv() => match msg {
                Some(Event::Handle(name, handle)) => {
                    assert!(!closed);
                    stream.insert(name, handle);
                },
                Some(Event::Closed) => closed = true,
                None => todo!("implies dropped, abort all "),
            }
        }
    }

    // let mut stream = ...
    // loop { select a) incoming handles, b) finished tasks }
}
