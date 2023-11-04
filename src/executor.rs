use futures::future::Future;
use futures::Canceled;
use futures::sync::oneshot::{channel, Sender};
use futures_cpupool::{Builder, CpuPool};
use tokio_core::reactor::Timeout;
use tokio_core::reactor::{Core, Handle, Remote};

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Instant, Duration};


#[derive(Clone)]
pub struct TaskHandle { //monitors if thread is stopped
    should_stop: Arc<AtomicBool>,
}

impl TaskHandle { // constructor
    fn new() -> TaskHandle {
        TaskHandle { should_stop: Arc::new(AtomicBool::new(false)) }
    }

    pub fn stop(&self) { //termination for a thread? ordering; relaxed no specific memory ordering constraints.
        self.should_stop.store(true, Ordering::Relaxed);
    }

    pub fn stopped(&self) -> bool { //checks if thread execution has stopped
        self.should_stop.load(Ordering::Relaxed)
    }
}

fn fixed_interval_loop<F>(scheduled_fn: F, interval: Duration, handle: &Handle, task_handle: TaskHandle)
    where F: Fn(&Handle) + Send + 'static
{//fixed_interval_loop is a recursive function designed to repeatedly execute a provided function at fixed intervals
    if task_handle.stopped() {
        return;
    }
    let start_time = Instant::now();
    scheduled_fn(handle);
    let execution = start_time.elapsed();
    let next_iter_wait = if execution >= interval {
        Duration::from_secs(0) //If the execution time exceeds the specified interval, no wait time is needed. makes sure nothing takes infinite time.
    } else {
        interval - execution
    };
    let handle_clone = handle.clone();
    let t = Timeout::new(next_iter_wait, handle).unwrap()
        .then(move |_| {
            fixed_interval_loop(scheduled_fn, interval, &handle_clone, task_handle);
            Ok::<(), ()>(())
        });
    handle.spawn(t);
}

fn calculate_delay(interval: Duration, execution: Duration, delay: Duration) -> (Duration, Duration) {
    if execution >= interval { 
        (Duration::from_secs(0), delay + execution - interval)
    } else {// handles the case where there is still time remaining in the current interval
        let wait_gap = interval - execution;
        if delay == Duration::from_secs(0) {
            (wait_gap, Duration::from_secs(0))
        } else if delay < wait_gap {
            (wait_gap - delay, Duration::from_secs(0))
        } else {
            (Duration::from_secs(0), delay - wait_gap)
        }
    }
}

fn fixed_rate_loop<F>(scheduled_fn: F, interval: Duration, handle: &Handle, delay: Duration, task_handle: TaskHandle)
    where F: Fn(&Handle) + Send + 'static
{//fixed_rate_loop is a function that schedules repeated execution of a provided function at a fixed rate, using the specified interval and initial delay
    if task_handle.stopped() {
        return;
    }
    let start_time = Instant::now();
    scheduled_fn(handle);
    let execution = start_time.elapsed();
    let (next_iter_wait, updated_delay) = calculate_delay(interval, execution, delay);
    let handle_clone = handle.clone();
    let t = Timeout::new(next_iter_wait, handle).unwrap()
        .then(move |_| {
            fixed_rate_loop(scheduled_fn, interval, &handle_clone, updated_delay, task_handle);
            Ok::<(), ()>(())
        });
    handle.spawn(t);//it schedules the Timeout by calling spawn on the handle. This means that when the Timeout fires, it will trigger the execution of the then closure, effectively scheduling the next iteration.
}


struct CoreExecutorInner { //single core executor 
    remote: Remote,
    termination_sender: Option<Sender<()>>,
    thread_handle: Option<JoinHandle<Result<(), Canceled>>>,
}

impl Drop for CoreExecutorInner { 
    fn drop(&mut self) {
        let _ = self.termination_sender.take().unwrap().send(());
        let _ = self.thread_handle.take().unwrap().join();
    }
}

pub struct CoreExecutor {
    inner: Arc<CoreExecutorInner>
}

impl Clone for CoreExecutor {
    fn clone(&self) -> Self {
        CoreExecutor { inner: Arc::clone(&self.inner) }
    }
}

impl CoreExecutor {
    pub fn new() -> Result<CoreExecutor, io::Error> {
        CoreExecutor::with_name("core_executor")
    }

    pub fn with_name(thread_name: &str) -> Result<CoreExecutor, io::Error> { //naming and initialization of thread and sets up communication channels
        let (termination_tx, termination_rx) = channel();
        let (core_tx, core_rx) = channel();
        let thread_handle = thread::Builder::new()
            .name(thread_name.to_owned())
            .spawn(move || {
                let mut core = Core::new().expect("Failed to start core");
                let _ = core_tx.send(core.remote());
                core.run(termination_rx)
            })?;
        let inner = CoreExecutorInner {
            remote: core_rx.wait().expect("Failed to receive remote"),
            termination_sender: Some(termination_tx),
            thread_handle: Some(thread_handle),
        };
        let executor = CoreExecutor {
            inner: Arc::new(inner)
        };
        Ok(executor)
    }

    pub fn schedule_fixed_interval<F>(&self, initial: Duration, interval: Duration, scheduled_fn: F) -> TaskHandle
        where F: Fn(&Handle) + Send + 'static //basically generates the time slices and the order of execution for the processes. It also determines when the next execution will take place.
    {
        let task_handle = TaskHandle::new();
        let task_handle_clone = task_handle.clone();
        self.inner.remote.spawn(move |handle| {
            let handle_clone = handle.clone();
            let t = Timeout::new(initial, handle).unwrap()
                .then(move |_| {
                    fixed_interval_loop(scheduled_fn, interval, &handle_clone, task_handle_clone);
                    Ok::<(), ()>(())
                });
            handle.spawn(t);
            Ok::<(), ()>(())
        });
        task_handle
    }

    pub fn schedule_fixed_rate<F>(&self, initial: Duration, interval: Duration, scheduled_fn: F) -> TaskHandle
        where F: Fn(&Handle) + Send + 'static
    {
        let task_handle = TaskHandle::new();
        let task_handle_clone = task_handle.clone();
        self.inner.remote.spawn(move |handle| {
            let handle_clone = handle.clone();
            let t = Timeout::new(initial, handle).unwrap()
                .then(move |_| {
                    fixed_rate_loop(scheduled_fn, interval, &handle_clone, Duration::from_secs(0), task_handle_clone);
                    Ok::<(), ()>(())
                });
            handle.spawn(t);
            Ok::<(), ()>(())
        });
        task_handle
    }
}


#[derive(Clone)]
pub struct ThreadPoolExecutor {
    executor: CoreExecutor,
    pool: CpuPool
}

impl ThreadPoolExecutor {
    pub fn new(threads: usize) -> Result<ThreadPoolExecutor, io::Error> {
        ThreadPoolExecutor::with_prefix(threads, "pool_thread_")
    }

    pub fn with_prefix(threads: usize, prefix: &str) -> Result<ThreadPoolExecutor, io::Error> {
        let new_executor = CoreExecutor::with_name(&format!("{}executor", prefix))?;
        Ok(ThreadPoolExecutor::with_executor(threads, prefix, new_executor))
    }

    pub fn with_executor(threads: usize, prefix: &str, executor: CoreExecutor) -> ThreadPoolExecutor {
        let pool = Builder::new()
            .pool_size(threads)
            .name_prefix(prefix)
            .create();
        ThreadPoolExecutor { pool, executor }
    }

    pub fn schedule_fixed_rate<F>(&self, initial: Duration, interval: Duration, scheduled_fn: F) -> TaskHandle
        where F: Fn(&Remote) + Send + Sync + 'static
    {
        let pool_clone = self.pool.clone(); //deep copy for ownership
        let arc_fn = Arc::new(scheduled_fn); 
        self.executor.schedule_fixed_interval(
            initial,
            interval,
            move |handle| {
                let arc_fn_clone = arc_fn.clone();
                let remote = handle.remote().clone();
                let t = pool_clone.spawn_fn(move || {
                    arc_fn_clone(&remote);
                    Ok::<(),()>(())
                });
                handle.spawn(t);
            }
        )
    }

    pub fn pool(&self) -> &CpuPool {
        &self.pool
    }
}
