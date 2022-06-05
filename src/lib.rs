mod stream;

use std::{borrow::Cow, error, fmt, future::Future};

use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tokio_stream::StreamExt;

use crate::stream::{JoinHandleStream, TaskError};

/// The name of a task spawned inside a group.
type TaskName = Cow<'static, str>;
/// The result of an entire [`TaskGroup`].
type GroupResult<E> = Result<(), TaskError<E>>;

/// A handle to a group of [`tokio::task`]s which runs until all tasks have
/// finished or until one task encounters an error.
///
/// Tasks spawned into a [`TaskGroup`] must return a `Result<(), E>`, if tasks
/// should return values on success as well, channels should be used.
pub struct TaskGroup<E> {
    /// The channel for spawning further tasks.
    tx: mpsc::UnboundedSender<Event<E>>,
    /// The receiver for the group's completion.
    done_rx: Option<oneshot::Receiver<GroupResult<E>>>,
}

impl<E: Send + 'static> TaskGroup<E> {
    /// Returns a new [`TaskGroup`].
    ///
    /// # Panics
    ///
    /// Panics, if called outside of a tokio runtime.
    pub fn new() -> Self {
        Self::with_capacity(0, false)
    }

    /// Returns a new local [`TaskGroup`], i.e., with a local manager task.
    ///
    /// # Panics
    ///
    /// Panics, if called outside of a tokio runtime.
    pub fn local() -> Self {
        Self::with_capacity(0, true)
    }

    /// Returns a new [`TaskGroup`] with an initial task handle `capacity`.
    pub fn with_capacity(capacity: usize, local: bool) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();

        if local {
            let _ = task::spawn_local(group_manager(capacity, done_tx, rx));
        } else {
            let _ = task::spawn(group_manager(capacity, done_tx, rx));
        }

        Self { tx, done_rx: Some(done_rx) }
    }

    /// Closes the [`TaskGroup`] to further task additions and returns a
    /// [`JoinGroupHandle`] for awaiting the group's completion.
    pub fn close(mut self) -> JoinGroupHandle<E> {
        let done_rx = self.done_rx.take().expect("task group error has already been observed");

        // send the close message to group manager
        let res = self.tx.send(Event::Closed);
        assert!(res.is_ok(), "group manager must be available for close msg");

        JoinGroupHandle { done_rx, _tx: self.tx }
    }

    /// Spawns a task for the given `future` and adds it to the task group.
    ///
    /// # Errors
    ///
    /// Fails, if the task group has previously encountered an error. In this
    /// case the spawned task will be cancelled.
    /// Once a `spawn` has failed, all subsequent ones will fail as well.
    /// The encountered error can be retrieved by calling
    /// [`errored`](TaskGroup::errored).
    pub fn spawn<F>(&self, name: impl Into<TaskName>, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        // can not spawn further tasks once an reported error has been observed.
        if self.done_rx.is_none() {
            return Err(SpawnError(name.into()));
        }

        // spawn the task and send its handle to the manager
        let handle = task::spawn(future);
        self.tx.send(Event::Handle(name.into(), handle)).map_err(|err| match err.0 {
            Event::Handle(name, handle) => {
                handle.abort();
                SpawnError(name)
            }
            Event::Closed => unreachable!(),
        })
    }

    /// Resolves when a task in the group has reported an error.
    ///
    /// # Cancel Safety
    ///
    /// ...
    ///
    /// # Panics
    ///
    /// Panics, if called again after observing an error.
    pub async fn errored(&mut self) -> TaskError<E> {
        let done_rx = self.done_rx.as_mut().expect("task group error has already been observed");
        tokio::pin!(done_rx);

        match done_rx.await {
            Ok(Ok(_)) | Err(_) => unreachable!("not possible while handle exists"),
            Ok(Err(err)) => {
                self.done_rx = None;
                err
            }
        }
    }
}

/// A handle for joining a closed [`TaskGroup`].
///
/// If the handle is dropped, all tasks are cancelled.
pub struct JoinGroupHandle<E> {
    done_rx: oneshot::Receiver<GroupResult<E>>,
    _tx: mpsc::UnboundedSender<Event<E>>,
}

impl<E> JoinGroupHandle<E> {
    /// Joins all remaining tasks in the [`TaskGroup`].
    pub async fn join(self) -> Result<(), TaskError<E>> {
        self.done_rx.await.expect("not possible while handle exists")
    }
}

/// The error returned when spawning into a [`TaskGroup`] fails.
#[derive(Debug)]
pub struct SpawnError(TaskName);

impl SpawnError {
    /// Returns the name of the task that failed to be spawned.
    pub fn name(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to spawn task '{}'", self.0)
    }
}

impl error::Error for SpawnError {}

enum Event<E> {
    Handle(TaskName, task::JoinHandle<Result<(), E>>),
    Closed,
}

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
            // 1) task has completed or group is closed & there are no more tasks
            res = &mut next => match res {
                // the completed task had an error
                Some(res) => {
                    if let Err(e) = res {
                        // close the channel to prevent spawning further tasks,
                        // but keep handling any that have already been queued,
                        // i.e., don't break out yet
                        rx.close();
                        group_res = Err(e);
                    }
                },
                // the task group has been closed and all tasks have finished
                // NOTE: this can only happen AFTER `stream` has been closed
                None => break,
            },
            msg = rx.recv() => match msg {
                Some(Event::Handle(name, handle)) => {
                    assert!(is_closed == false, "close event already received");
                    stream.insert(name, handle)
                },
                Some(Event::Closed) => {
                    // closing the stream will end it once all currently stored
                    // task handles have been joined
                    is_closed = true;
                    stream.close();
                },
                None => {
                    // if the group or join handle is dropped, cancel all tasks
                    stream.abort_all().await;
                    break;
                },
            }
        }
    }

    // send the result to the callsite
    let _ = done_tx.send(group_res);
}

#[cfg(test)]
fn block_on<F: Future<Output = ()> + Send>(f: F) {
    tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(f)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicI32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use crate::stream::TaskError;

    use super::TaskGroup;

    struct OnCancel(Arc<AtomicI32>);

    impl Drop for OnCancel {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn close() {
        crate::block_on(async {
            let group = TaskGroup::<()>::new();

            group.spawn("a", async { Ok(()) }).unwrap();
            group.spawn("b", async { Ok(()) }).unwrap();
            group.spawn("c", async { Ok(()) }).unwrap();

            let handle = group.close();
            let res = handle.join().await;
            assert!(res.is_ok())
        })
    }

    #[test]
    fn cancel() {
        crate::block_on(async {
            let group = TaskGroup::<()>::new();

            let cancelled = Arc::default();
            let flag = Arc::clone(&cancelled);
            group
                .spawn("a", async move {
                    let _guard = OnCancel(flag);
                    std::future::pending().await
                })
                .unwrap();

            // yielding before & after allows the runtime
            // to schedule the other tasks
            tokio::task::yield_now().await;
            drop(group);
            tokio::task::yield_now().await;

            assert_eq!(cancelled.load(Ordering::Relaxed), 1);
        })
    }

    #[test]
    fn keep_spawning() {
        crate::block_on(async {
            let mut group = TaskGroup::<i32>::new();
            let mut timer = tokio::time::interval(Duration::from_millis(1));

            let mut i = 0;
            let counter = Arc::new(AtomicI32::new(0));

            let err = loop {
                tokio::select! {
                    err = group.errored() => break err,
                    _ = timer.tick() => {
                        let counter = Arc::clone(&counter);
                        if i < 10 {
                            group.spawn(format!("t-{i}"), async move {
                                let _guard = OnCancel(counter);
                                std::future::pending().await
                            }).unwrap();
                        } else if i == 10 {
                            group.spawn(format!("t-{i}"), async {
                                Err(-1)
                            }).unwrap();
                        }

                        i += 1;
                    }
                }
            };

            assert_eq!(err.task_name(), "t-10");
            assert!(matches!(err, TaskError::Error(_, -1)));

            tokio::task::yield_now().await;

            assert_eq!(counter.load(Ordering::Relaxed), 10);
        })
    }

    #[test]
    fn keep_running() {
        crate::block_on(async {
            let group = TaskGroup::<i32>::new();
            let counter = Arc::new(AtomicI32::new(0));

            {
                let counter = Arc::clone(&counter);
                group
                    .spawn("a", async move {
                        let _guard = OnCancel(counter);
                        std::future::pending().await
                    })
                    .unwrap();
            }

            // close the group, this must not cancel the task
            let handle = group.close();
            tokio::task::yield_now().await;
            assert_eq!(counter.load(Ordering::Relaxed), 0);

            // drop the join handle, this MUST cancel the task
            drop(handle);
            tokio::task::yield_now().await;
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        });
    }
}
