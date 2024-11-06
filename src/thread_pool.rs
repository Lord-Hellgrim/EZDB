use std::{collections::{HashMap, VecDeque}, net::TcpStream, sync::{Arc, Condvar, Mutex}, thread::JoinHandle};


use crate::{db_structure::KeyString, server_networking::{answer_query, perform_administration, perform_maintenance}, utilities::{SocketSide, CsPair}};


pub type JobQueue = Mutex<VecDeque<Job>>;
pub type OpenSockets = HashMap<KeyString, SocketSide>;
pub type ResultQueue = Mutex<VecDeque<(KeyString, Vec<u8>)>>;

pub struct Job {
    peer: KeyString,
    cs_pair: CsPair,
    data: Vec<u8>,
}


pub struct EzThreadPool {
    pub threads: HashMap<usize, JoinHandle<()>>,
    pub job_queue: Arc<Mutex<VecDeque<Job>>>,
    pub result_queue: Arc<Mutex<VecDeque<(KeyString, Vec<u8>)>>>,
}

impl EzThreadPool {
    pub fn initialize(number_of_threads: usize) -> EzThreadPool {

        let mut threads = HashMap::new();
        let job_queue: Arc<Mutex<VecDeque<Job>>> = Arc::new(Mutex::new(VecDeque::new()));

        let result_queue = Arc::new(Mutex::new(VecDeque::new()));

        for i in 0..number_of_threads {
            let jobs = job_queue.clone();
            let results = result_queue.clone();
            // println!("spawned thread: {}", i);
            let thread = std::thread::spawn(move || {
                loop {
                    // println!("Awoken!");
                    let mut jobs_lock = jobs.lock().unwrap();
                    let job = jobs_lock.pop_front();
                    match job {
                        Some(mut job) => {
                            drop(jobs_lock);
                            let data = match job.cs_pair.c1.DecryptWithAd(&[], &job.data) {
                                Ok(x) => x,
                                Err(e) => {
                                    println!("Could not decrypt job data");
                                    continue
                                },
                            };
                            let result = match KeyString::try_from(&data[0..64]) {
                                Ok(s) => match s.as_str() {
                                    "QUERY" => answer_query(data),
                                    "ADMIN" => perform_administration(data),
                                    action => {
                                        println!("Asked to perform unsupported action: '{}'", action);
                                        continue
                                    }
                                },
                                Err(_) => {
                                    println!("Could not parse first 64 bytes as a KeyString");
                                    continue
                                },
                            };
                            match result {
                                Ok(r) => {
                                    let r = job.cs_pair.c2.EncryptWithAd(&[], &r);
                                    results.lock().unwrap().push_back((job.peer, r));
                                },
                                Err(_) => {
                                    println!("Encountered an error while trying to carry out action");
                                    continue
                                },
                            }
                            
                        },
                        None => {
                            drop(jobs_lock);
                            perform_maintenance().unwrap();
                            std::thread::park()
                        },
                    };
                }
            });
            threads.insert(i, thread);
        }

        EzThreadPool {
            threads,
            job_queue,
            result_queue: result_queue,
        }
    }


}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    

}