use std::thread;
use std::sync::{mpsc, Arc, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { 
            workers, 
            sender: Some(sender), 
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    println!("Worker {id} got a job: executing.");
                    
                    job();
                }
                Err(_) => {
                    println!("Worker {id} disconnected; shutting down.");
                    break;
                }
            }


        });

        Worker { 
            id, 
            thread: Some(thread) }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::{TcpListener, TcpStream}, time::Duration, io::{Write, Read}};

    use super::*;

    #[test]
    fn test_ThreadPool() {

        fn handle_connection(mut stream: &mut TcpStream) {
            let mut buffer = [0u8;10];
            stream.read(&mut buffer);
            println!("buffer: {:x?}", buffer);
        }

        let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
        let pool = ThreadPool::new(4);

        thread::spawn(|| {
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
            thread::sleep(Duration::from_secs(1));
            let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
            stream.write("jim".as_bytes());
        });

        let instant = std::time::Instant::now();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            if instant.elapsed().as_secs() > 4 {
                break
            }
            pool.execute(move || {
                handle_connection(&mut stream);
            });
        }

        println!("Shutting down.");
    }
}