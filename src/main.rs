mod task_group;
mod executor;

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::task_group::{TaskGroup, TaskGroupScheduler};
use crate::executor::ThreadPoolExecutor;

struct FibonacciGroup {
    n: usize, //problem size
    result_lock: Arc<RwLock<Option<u64>>>, //result locked with rw
}

impl FibonacciGroup {
    fn new(n: usize) -> FibonacciGroup {
        FibonacciGroup { //initialization for groups
            n,
            result_lock: Arc::new(RwLock::new(None)),
        }
    }

    fn result_lock(&self) -> Arc<RwLock<Option<u64>>> {
        self.result_lock.clone() //returns a deep copy, original may be lost 
    }
}

impl TaskGroup for FibonacciGroup {
    type TaskId = ();

    fn get_tasks(&self) -> Vec<()> {
        vec![(); 1]
    }

    fn execute(&self, _task_id: ()) {
        let result = fibonacci(self.n); //
        *self.result_lock.write().unwrap() = Some(result);
    }
}

fn fibonacci(n: usize) -> u64 {
    if n <= 1 {
        return n as u64;
    }

    fibonacci(n - 1) + fibonacci(n - 2)
}


fn main() {
    let n = 35;

    let group = FibonacciGroup::new(n);
    let result_lock = group.result_lock(); //read write lock to avoid concurrency issues

    {
        let executor = ThreadPoolExecutor::new(4).unwrap();
        executor.schedule(group, Duration::from_secs(0), Duration::from_secs(4));
        thread::sleep(Duration::from_secs(1)); //synchronous
    }

    let result = *result_lock.read().unwrap(); 

    if let Some(result) = result {
        println!("The {}th Fibonacci number is: {}", n, result);
    } else {
        println!("Failed to calculate the Fibonacci number.");
    }
}