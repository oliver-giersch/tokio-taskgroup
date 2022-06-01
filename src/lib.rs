mod stream;

use std::{borrow::Cow, future::Future};

use stream::TaskError;
use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tokio_stream::StreamExt;

use crate::stream::JoinHandleStream;

type TaskName = Cow<'static, str>;

// TODO: handle sender
// TODO: error receiver (mut)
pub struct TaskGroup<E> {
    tx: mpsc::UnboundedSender<Event<E>>,
    done_rx: Option<oneshot::Receiver<GroupResult<E>>>,
}

impl<E: Send + 'static> TaskGroup<E> {
    /// # Panics
    ///
    /// Panics, if this is called outside of a tokio runtime.
    pub fn new() -> Self {
        Self::with_capacity(0, false)
    }

    pub fn local() -> Self {
        Self::with_capacity(0, true)
    }

    pub fn with_capacity(capacity: usize, local: bool) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();

        if local {
            let _ = task::spawn_local(group_manager(capacity, done_tx, rx));
        } else {
            let _ = task::spawn(group_manager(capacity, done_tx, rx));
        }

        Self {
            tx,
            done_rx: Some(done_rx),
        }
    }

    /// Closes the [`TaskGroup`]
    pub fn close(mut self) -> JoinGroupHandle<E> {
        let done_rx = self
            .done_rx
            .take()
            .expect("task group error has already been observed");

        // send the close message to group manager
        let res = self.tx.send(Event::Closed);
        assert!(res.is_ok(), "group manager must be available for close msg");

        JoinGroupHandle { done_rx }
    }

    pub fn spawn<F>(&self, name: impl Into<TaskName>, future: F)
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        // spawn
        let handle = task::spawn(future);
        let event = Event::Handle(name.into(), handle);

        let res = self.tx.send(event);
        assert!(res.is_ok(), "spawning must always succeed");
    }

    /// ...
    ///
    /// # Panics
    ///
    /// ...
    pub async fn errored(&mut self) -> (TaskName, TaskError<E>) {
        let done_rx = self
            .done_rx
            .take()
            .expect("task group error has already been observed");

        match done_rx.await {
            Ok(Ok(_)) | Err(_) => unreachable!("TODO: EXPLAIN"),
            Ok(Err((name, err))) => (name, err),
        }
    }

    // TODO: can fail, even if not close
}

pub struct JoinGroupHandle<E> {
    done_rx: oneshot::Receiver<GroupResult<E>>,
}

impl<E> JoinGroupHandle<E> {
    pub async fn join(self) -> Result<(), (TaskName, TaskError<E>)> {
        self.done_rx.await.expect("TODO")
    }
}

#[derive(Debug)]
pub struct SpawnError;

enum Event<E> {
    Handle(TaskName, task::JoinHandle<Result<(), E>>),
    Closed,
}

type GroupResult<E> = Result<(), (TaskName, TaskError<E>)>;

async fn group_manager<E>(
    capacity: usize,
    done_tx: oneshot::Sender<GroupResult<E>>,
    mut rx: mpsc::UnboundedReceiver<Event<E>>,
) {
    let mut stream = JoinHandleStream::<E>::with_capacity(capacity);
    let mut is_closed = false;

    let mut group_res: GroupResult<E> = Ok(());
    loop {
        let next = stream.next();
        tokio::pin!(next);

        tokio::select! {
            biased;
            // a task has completed or there are no more tasks
            res = &mut next => match res {
                // the completed task had an error
                Some(res) => if let Err(e) = res {
                    // close the channel to prevent spawning further tasks, but
                    // keep handling any that have already been queued, i.e.,
                    // don't break yet
                    rx.close();
                    group_res = Err(e);
                },
                // the task group has been closed and all tasks have finished
                None => break,
            },
            msg = rx.recv() => match msg {
                Some(Event::Handle(name, handle)) => {
                    assert!(is_closed == false);
                    stream.insert(name, handle)
                },
                Some(Event::Closed) => {
                    is_closed = true;
                    rx.close();
                    stream.close();
                },
                None => {
                    // if the group handle is just dropped (without sending a
                    // close message), cancel all tasks
                    stream.abort_all().await;
                    break;
                },
            }
        }
    }

    let _ = done_tx.send(group_res);

    // todo: cancel all handles still in rx
}

fn convert_task_result() {}

#[cfg(test)]
fn block_on<F: Future<Output = ()> + Send>(f: F) {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(f)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::TaskGroup;

    #[test]
    fn close() {
        crate::block_on(async {
            let group = TaskGroup::<()>::new();

            group.spawn("a", async { Ok(()) });
            group.spawn("b", async { Ok(()) });
            group.spawn("c", async { Ok(()) });

            let handle = group.close();
            let res = handle.join().await;
            assert!(res.is_ok())
        })
    }

    #[test]
    fn cancel() {
        struct OnCancel(Arc<AtomicBool>);

        impl Drop for OnCancel {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        crate::block_on(async {
            let cancelled = Arc::default();

            let group = TaskGroup::<()>::new();

            let flag = Arc::clone(&cancelled);
            group.spawn("a", async move {
                let _guard = OnCancel(flag);
                std::future::pending().await
            });

            drop(group);
            tokio::task::yield_now().await;

            assert!(cancelled.load(Ordering::Relaxed));
        })
    }
}
