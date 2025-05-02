use std::{collections::{HashMap, VecDeque}, os::fd::AsRawFd, sync::{Arc, Condvar, Mutex}};


use crate::{server_networking::{answer_kv_query, answer_query, perform_administration, perform_maintenance, Database}, utilities::{ksf, KeyString}};


pub struct Job {
    pub connection: eznoise::Connection,
    pub data: Vec<u8>,
}


pub struct ThreadHandler {
    pub jobs_condvar: Arc<Condvar>,
    pub job_queue: Arc<Mutex<VecDeque<Job>>>,
    pub open_connections: Arc<Mutex<HashMap<u64, eznoise::Connection>>>,
}

impl ThreadHandler {
    pub fn push_job(&self, job: Job) {
        self.job_queue.lock().unwrap().push_back(job);
        self.jobs_condvar.notify_one();
    }

}

pub fn initialize_thread_pool(number_of_threads: usize, db_ref: Arc<Database>) -> ThreadHandler {

    let job_queue: Arc<Mutex<VecDeque<Job>>> = Arc::new(Mutex::new(VecDeque::new()));

    let open_connections = Arc::new(Mutex::new(HashMap::new()));

    let jobs_queue_condvar = Arc::new(Condvar::new());
    
    for _ in 0..number_of_threads {
        let jobs = job_queue.clone();

        let open_connections_clone = open_connections.clone();

        let jobs_condvar = jobs_queue_condvar.clone();

        let thread_db_ref = db_ref.clone();
        std::thread::spawn(move || {
            
            loop {
                let loop_db_ref = thread_db_ref.clone();

                let mut job_lock = jobs.lock().unwrap();
                let job = job_lock.pop_front();
                match job {
                    Some(mut job) => {
                        drop(job_lock);
                        let data = match job.connection.c1.DecryptWithAd(&[], &job.data) {
                            Ok(x) => x,
                            Err(_) => {
                                println!("Could not decrypt job data");

                                ksf("Couldn't decrypt").raw().to_vec()
                            },
                        };
                        println!("data: {:?}", &data[64..]);
                        let result = match KeyString::try_from(&data[0..64]) {
                            Ok(s) => match s.as_str() {
                                "QUERY" => answer_query(&data[64..], &mut job.connection, loop_db_ref),
                                "ADMIN" => perform_administration(&data[64..], loop_db_ref),
                                "KVQUERY" => answer_kv_query(&data[64..], &mut job.connection, loop_db_ref),
                                action => {
                                    println!("Asked to perform unsupported action: '{}'", action);

                                    Ok(s.raw().to_vec())
                                }
                            },
                            Err(e) => {
                                println!("Could not parse first 64 bytes as a KeyString");

                                Err(e)
                                
                            },
                        };
                        match result {
                            Ok(r) => {
                                match job.connection.SEND_C2(&r) {
                                    Ok(_) => (),
                                    Err(_) => println!("Noise Error line {}, column {}", line!(), column!()),
                                };
                                
                            },
                            Err(e) => {
                                println!("Encountered an error while trying to carry out action");

                                match job.connection.SEND_C2(&format!("Encountered an error while trying to carry out action.\n Error: '{}'", e).as_bytes()) {
                                    Ok(_) => (),
                                    Err(_) => println!("Noise Error line {}, column {}", line!(), column!()),
                                };
                            },
                        };
                        open_connections_clone.lock().unwrap().insert(job.connection.stream.as_raw_fd() as u64, job.connection);
                        
                    },
                    None => {
                        perform_maintenance(loop_db_ref).unwrap();
                        job_lock = jobs_condvar.wait(job_lock).unwrap();
                    },
                }
                
            }

        });
    }

    ThreadHandler {
        jobs_condvar: jobs_queue_condvar,
        job_queue: job_queue,
        open_connections,

    }

}