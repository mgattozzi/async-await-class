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
        while let Ok(task) = self.task_receiver.recv() {
            let mut future_slot = task.future.lock().unwrap();
            // Take the future, and if it has not yet completed (is still Some)
            // poll it in an attempt to complete it.
            if let Some(mut future) = future_slot.take() {
                // The waker is the `Task` itself
                let waker = local_waker_from_nonlocal(task.clone());

                // The spawner is the one associated with the task.
                // This means that new futures spawned from inside this
                // future will be placed onto the same task queue.
                let mut spawn = &task.spawner;
                let cx = &mut task::Context::new(&waker, &mut spawn);
                if let Poll::Pending = PinMut::new(&mut future).poll(cx) {
                    // We're not done processing the future, so put it
                    // back in its task to be run again in the future.
                    *future_slot = Some(future);
                }
            }
        }
    }
}

/// Task executor that spawns tasks onto a channel.
#[derive(Clone)]
struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        (&mut &*self).spawn_obj(FutureObj::new(Box::new(future)))
            .expect("unable to spawn");
    }
}

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
        let cloned = arc_self.clone();
        let _ = arc_self.spawner.task_sender.send(cloned);
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
