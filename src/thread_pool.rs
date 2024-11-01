use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}};

use crate::db_structure::KeyString;


pub struct ThreadHandler {
    pairs: Vec<Arc<(Mutex<bool>, Condvar)>>,
    job_queue: Arc<Mutex<Vec<Vec<u8>>>>,
    results_queue: Arc<Mutex<Vec<Vec<u8>>>>,
}


pub fn initialize_thread_pool(number_of_threads: usize) -> ThreadHandler {

    let mut lock_condvar_pairs: Vec<Arc<(Mutex<bool>, Condvar)>> = Vec::with_capacity(number_of_threads);
    for _ in 0..number_of_threads {
        lock_condvar_pairs.push(Arc::new((Mutex::new(false), Condvar::new())));
    }

    let job_queue: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

    let result_queue = Arc::new(Mutex::new(Vec::new()));

    for i in 0..number_of_threads {
        let thread_pair = lock_condvar_pairs[i].clone();
        let jobs = job_queue.clone();
        let results = result_queue.clone();
        std::thread::spawn(move || {
            let (lock, cvar) = &*thread_pair;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cvar.wait(started).unwrap();
                
                
                
                *started = false;
            }
        });
    }

    ThreadHandler {
        pairs: lock_condvar_pairs,
        job_queue: job_queue,
        results_queue: result_queue,
    }

}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_thread_pool() {
        let handler = initialize_thread_pool(4);

        for i in 0..4 {
            std::thread::sleep(Duration::from_secs(1));
            let mut lock = handler.job_queue.lock().unwrap();
            lock.push(vec![i*1,i*2,i*3,i*4,i*5]);
            let mut start = handler.pairs[i as usize].0.lock().unwrap();
            *start = true;
            handler.pairs[i as usize].1.notify_one();
            drop(lock);
        }
        std::thread::sleep(Duration::from_secs(1));


        for list in handler.results_queue.lock().unwrap().iter() {
            println!("list: {:?}", list);
        }

    }

}