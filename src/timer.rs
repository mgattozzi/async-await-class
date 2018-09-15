#![feature(arbitrary_self_types, futures_api, pin)]

use std::future::Future;
use std::marker::Unpin;
use std::pin::PinMut;
use std::sync::{Arc, Mutex};
use std::task::{self, Poll, Waker};
use std::thread; // You'll want to use `spawn` and `sleep`.
use std::time::Duration;

mod secret;
use self::secret::run_future;

struct MyTimerFuture {
    sleep_duration: Duration,
    first: bool,
    shared_state: Arc<Mutex<SharedState>>,
}

// State shared between the future and your thread.
struct SharedState {
    completed: bool,
    waker: Option<Waker>,
}

// Pinning will be covered later-- for now, it's enough to understand
// that our type doesn't require it, so it is `Unpin`.
impl Unpin for MyTimerFuture {}

impl Future for MyTimerFuture {
    type Output = ();
    fn poll(self: PinMut<Self>, cx: &mut task::Context)
        -> Poll<Self::Output>
    {
        {
            let mut shared_state = self.shared_state.lock().unwrap();
            if shared_state.completed {
                return Poll::Ready(())
            } else {
                // FIXME: add code to tell the thread how to wake up
                // the current future's task. This will involve
                // changing `shared_state`.
            }
        }

        if self.first {
            // This is the first time the future has been run,
            // so the duration starts from here.
            // We start a new thread to keep track of the time
            // for us and wake us up when the timeout is over.
            //
            // This prevents the event loop from being blocked while
            // our timer waits.
            //
            // note: this is not an efficient way to implement a real-world timer. ;)
            let shared_state = self.shared_state.clone();
            let duration = self.sleep_duration.clone();
            thread::spawn(move || {
                thread::sleep(duration);
                // FIXME: cause the future to complete.
                // This will involve changing `shared_state`.
            });
        }

        Poll::Pending
    }
}

fn main() {
    let future = MyTimerFuture {
        sleep_duration: Duration::from_secs(2),
        first: true,
        shared_state: Arc::new(Mutex::new(SharedState {
            completed: false,
            waker: None
        })),
    };
    run_future(future);
}
