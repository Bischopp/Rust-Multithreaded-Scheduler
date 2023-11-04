use futures::future::Future;
use futures_cpupool::CpuPool;
use tokio_core::reactor::{Handle, Remote, Timeout};

use crate::executor::{CoreExecutor, ThreadPoolExecutor};

use std::sync::Arc;
use std::time::Duration;


pub trait TaskGroup: Send + Sync + Sized + 'static {
    type TaskId: Send;

    fn get_tasks(&self) -> Vec<Self::TaskId>;

    fn execute(&self, task_id: Self::TaskId);
}

fn schedule_tasks_local<T: TaskGroup>(task_group: &Arc<T>, interval: Duration, handle: &Handle) { // for a single thread
    let tasks = task_group.get_tasks();
    if tasks.is_empty() {
        return
    }
    let task_interval = interval / tasks.len() as u32;
    for (i, task) in tasks.into_iter().enumerate() {
        let task_group_clone = task_group.clone();
        let t = Timeout::new(task_interval * i as u32, handle).unwrap()
            .then(move |_| {
                task_group_clone.execute(task);
                Ok::<(), ()>(())
            });
        handle.spawn(t);
    }
}

fn schedule_tasks_remote<T: TaskGroup>(task_group: &Arc<T>, interval: Duration, remote: &Remote, pool: &CpuPool) { // for a whole pool of threads
    let tasks = task_group.get_tasks();
    if tasks.is_empty() {
        return
    }
    let task_interval = interval / tasks.len() as u32;
    for (i, task) in tasks.into_iter().enumerate() {
        let task_group = task_group.clone();
        let pool = pool.clone();

        remote.spawn(move |handle| {
            let task_group = task_group.clone();
            let pool = pool.clone();
            let t = Timeout::new(task_interval * i as u32, handle).unwrap()
                .then(move |_| {
                    task_group.execute(task);
                    Ok::<(), ()>(())
                });
            handle.spawn(pool.spawn(t));
            Ok::<(), ()>(())
        })
    }
}

pub trait TaskGroupScheduler {
    fn schedule<T: TaskGroup>(&self, task_group: T, initial: Duration, interval: Duration) -> Arc<T>;
}

impl TaskGroupScheduler for CoreExecutor {
    fn schedule<T: TaskGroup>(&self, task_group: T, initial: Duration, interval: Duration) -> Arc<T> {
        let task_group = Arc::new(task_group);
        let task_group_clone = task_group.clone();
        self.schedule_fixed_rate(
            initial,
            interval,
            move |handle| {
                schedule_tasks_local(&task_group_clone, interval, handle);
            }
        );
        task_group
    }
}

impl TaskGroupScheduler for ThreadPoolExecutor {
    fn schedule<T: TaskGroup>(&self, task_group: T, initial: Duration, interval: Duration) -> Arc<T> {
        let task_group = Arc::new(task_group);
        let task_group_clone = task_group.clone();
        let pool = self.pool().clone();
        self.schedule_fixed_rate(
            initial,
            interval,
            move |remote| {
                schedule_tasks_remote(&task_group_clone, interval, remote, &pool);
            }
        );
        task_group
    }
}