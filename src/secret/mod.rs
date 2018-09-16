#![allow(warnings)]

use std::time::{Duration, Instant};
use std::future::{Future, FutureObj};
use std::marker::Unpin;
use std::pin::PinMut;
use std::sync::{Arc, Mutex, Condvar};
use std::task::{
    self,
    local_waker_from_nonlocal,
    Poll,
    Spawn,
    SpawnObjError,
    Wake,
};

pub struct AlmostReady {
    ready: bool,
    value: i32,
}

pub fn almost_ready(value: i32) -> AlmostReady {
    AlmostReady { ready: false, value }
}

impl Future for AlmostReady {
    type Output = i32;
    fn poll(mut self: PinMut<Self>, cx: &mut task::Context)
        -> Poll<Self::Output>
    {
        if self.ready {
            Poll::Ready(self.value)
        } else {
            self.ready = true;
            cx.waker().wake();
            Poll::Pending
        }
    }
}

struct DebugWaker {
    condvar: Condvar,
    awoken: Mutex<bool>,
}

impl Wake for DebugWaker {
    fn wake(arc_self: &Arc<Self>) {
        println!("`Waker::wake` called");
        {
            let mut lock = arc_self.awoken.lock().unwrap();
            *lock = true;
            arc_self.condvar.notify_one();
        }
    }
}

struct ErrorSpawn;
impl Spawn for ErrorSpawn {
    fn spawn_obj(&mut self, _: FutureObj<'static, ()>)
        -> Result<(), SpawnObjError>
    {
        panic!(
            "Hey! It looks like you're trying to spawn a task as part of the \
            timer examples. We'll learn about task spawning later, so hold on \
            tight. If you're done, help out a neighbor! If not, then \
            \"this is not the solution you're looking for\".")
    }
}

/// Runs a future to completion.
pub fn run_future(mut future: impl Future<Output = ()> + Unpin) {
    let spawn = &mut ErrorSpawn;
    let waker = Arc::new(DebugWaker {
        condvar: Condvar::new(),
        awoken: Mutex::new(false),
    });
    let local_waker = local_waker_from_nonlocal(waker.clone());
    let cx = &mut task::Context::new(&local_waker, spawn);
    let mut future = PinMut::new(&mut future);
    println!("Beginning to run future!");
    let start = Instant::now();
    loop {
        match future.reborrow().poll(cx) {
            Poll::Ready(()) => {
                println!("Completed future after {:?}", Instant::now() - start);
                return
            }
            Poll::Pending =>
                println!("Future returned pending after {:?}", Instant::now() - start),
        }
        let mut awoken = waker.awoken.lock().unwrap();
        let was_awoken = *awoken;
        *awoken = false;
        if was_awoken {
            println!("The Future was immediately awoken");
            continue;
        }
        println!("Going to sleep...");
        let mut awoken = awoken;
        loop {
            let (awoken_local, timed_out) =
                waker.condvar.wait_timeout(awoken, Duration::from_secs(5)).unwrap();
            awoken = awoken_local;
            if timed_out.timed_out() {
                println!("Slept for more than 5 seconds without wakeup, exiting...");
                return;
            }
            if *awoken {
                *awoken = false;
                break;
            }
        }
    }
}
