use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}, thread::JoinHandle};

use crate::{db_structure::KeyString, ezql::execute_EZQL_queries, server_networking::{answer_query, perform_administration}};


pub type MultiQueue = Arc<Mutex<HashMap<KeyString, Job>>>;

pub enum Job {
    Query(Vec<u8>),
    Admin(KeyString),
}

pub struct ThreadHandler {
    pub pairs: Vec<Arc<(Mutex<bool>, Condvar)>>,
    pub job_queue: Arc<Mutex<Vec<Vec<u8>>>>,
    pub results_queue: Arc<Mutex<Vec<Vec<u8>>>>,
}

pub struct EzThreadPool {
    pub threads: Vec<JoinHandle<()>>,
    pub job_queue: Arc<Mutex<Vec<(KeyString, Job)>>>,
    pub result_queue: MultiQueue,
}

#[inline]
pub fn new_multiqueue() -> MultiQueue {
    Arc::new(Mutex::new(HashMap::new()))
}


pub fn create_thread_pool(number_of_threads: usize) -> EzThreadPool {

    let mut threads = Vec::new();
    let job_queue: Arc<Mutex<Vec<(KeyString, Job)>>> = Arc::new(Mutex::new(Vec::new()));

    let result_queue = new_multiqueue();

    for i in 0..number_of_threads {
        let jobs = job_queue.clone();
        let results = result_queue.clone();
        // println!("spawned thread: {}", i);
        let thread = std::thread::spawn(move || {
            loop {
                // println!("Awoken!");
                let mut jobs_lock = jobs.lock().unwrap();
                let job = jobs_lock.pop();
                match job {
                    Some(job) => {
                        drop(jobs_lock);
                        let (peer, job) = job;
                        let result = match job {
                            Job::Query(binary) => answer_query(binary),
                            Job::Admin(key_string) => perform_administration(key_string),
                        };
                    },
                    None => {
                        drop(jobs_lock);
                        std::thread::park()
                    },
                };
            }
        });
        threads.push(thread);
    }

    EzThreadPool {
        threads,
        job_queue,
        result_queue,
    }

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
            println!("spawned thread: {}", i);
            let (lock, cvar) = &*thread_pair;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cvar.wait(started).unwrap();
                println!("Awoken!");
                let mut jobs_lock = jobs.lock().unwrap();
                let job = jobs_lock.pop().unwrap();
                drop(jobs_lock);

                let result = job.into_iter().rev().collect();
                let mut rlock = results.lock().unwrap();
                rlock.push(result);
                drop(rlock);
                
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
    fn test_thread_handler() {
        let handler = initialize_thread_pool(4);

        for i in 0..4 {
            let mut lock = handler.job_queue.lock().unwrap();
            lock.push(vec![i*1,i*2,i*3,i*4,i*5]);
            let mut start = handler.pairs[i as usize].0.lock().unwrap();
            *start = true;
            handler.pairs[i as usize].1.notify_one();
            drop(lock);
        }
        std::thread::sleep(Duration::from_millis(1));


        for list in handler.results_queue.lock().unwrap().iter() {
            println!("list: {:?}", list);
        }
    }

}