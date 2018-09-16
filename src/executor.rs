#![feature(async_await, await_macro, futures_api, pin, arbitrary_self_types)]

use std::future::{Future, FutureObj};
use std::pin::PinMut;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{sync_channel, SyncSender, SendError, Receiver};
use std::task::{
    self,
    local_waker_from_nonlocal,
    Poll,
    Spawn,
    SpawnErrorKind,
    SpawnObjError,
    Wake,
};

mod secret;
use self::secret::almost_ready;

/// Task executor that receives tasks off of a channel and runs them.
struct Executor {
    task_receiver: Receiver<Arc<Task>>,
}

impl Executor {
    fn run(&self) {
        // FIXME: implement the running of the executor.
        //
        // This method should pull tasks off of the existing task
        // queue and run them to completion.
        //
        // In order to poll futures, you'll need to construct a
        // `task::Context` from a `LocalWaker` and a `Spawn`.
        // You can get a value of type `LocalWaker` by calling
        // `local_waker_from_nonlocal` on an `Arc<W>` where `W: Wake`.
        //
        // To poll the future you'll need to do:
        // `PinMut::new(future).poll(cx)` where cx is `&mut Context`
    }
}

/// Task executor that spawns tasks onto a channel.
#[derive(Clone)]
struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    // Spawn a future as a new top-level task.
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        let future_obj = FutureObj::new(Box::new(future));
        (&mut &*self).spawn_obj(future_obj)
            .expect("unable to spawn");
    }
}

// Implement the `Spawn` trait for `&Spawner` rather than `Spawner` since
// we don't require a mutable reference to `Spawner`.
impl<'a> Spawn for &'a Spawner {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>)
        -> Result<(), SpawnObjError>
    {
        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            spawner: self.clone(),
        });

        self.task_sender.send(task).map_err(|SendError(task)| {
            SpawnObjError {
                kind: SpawnErrorKind::shutdown(),
                future: task.future.lock().unwrap().take().unwrap(),
            }
        })
    }
}

struct Task {
    // In-progress future that should be pushed to completion
    future: Mutex<Option<FutureObj<'static, ()>>>,
    // Handle to spawn tasks onto the task queue
    spawner: Spawner,
}

impl Wake for Task {
    fn wake(arc_self: &Arc<Self>) {
        // FIXME: implement `Wake` by putting the task back onto the task queue
    }
}

fn new_executor_and_spawner() -> (Executor, Spawner) {
    // Maximum number of tasks to allow queueing in the channel at once.
    // This is just to make `sync_channel` happy, and wouldn't be present in
    // a real executor.
    const MAX_QUEUED_TASKS: usize = 10000;
    let (task_sender, task_receiver) = sync_channel(MAX_QUEUED_TASKS);
    (Executor { task_receiver }, Spawner { task_sender })
}

fn main() {
    let (executor, spawner) = new_executor_and_spawner();
    spawner.spawn(async {
        println!("howdy!");
        let x = await!(almost_ready(5));
        println!("done: {:?}", x);
    });
    executor.run();
}
