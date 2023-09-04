use std::fmt::{Display, self};
use std::fs;
use std::mem::size_of_val;

#[cfg(target_os="windows")]
pub const PATH_SEP: char = '\\';
#[cfg(target_os="linux")]
pub const PATH_SEP: char = '/';

pub struct LogTimeStamp {
    time: u64,
}

impl Display for LogTimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", unix_time_to_real(self.time))
    }
}

pub fn get_current_time() -> u64 {

    let x = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    x

}

pub fn unix_time_to_real(seconds: u64) -> String {
    const YEAR: u64 = 365 * 24 * 60 * 60;
    const DAY: u64 = 24 * 60 * 60;
    const HOUR: u64 = 60 * 60;
    const MINUTE: u64 = 60;

    let mut days = (seconds%YEAR - ((seconds / YEAR)/4) * DAY) / DAY + 1;
    let months: u64;
    
    match days {
        1..=31 => { months = 1;}
        32..=59 => {months = 2; days -= 31}
        60..=90 => {months = 3; days -= 59}
        91..=120 => {months = 4; days -= 90}
        121..=151 => {months = 5; days -= 120}
        152..=181 => {months = 6; days -= 151}
        182..=212 => {months = 7; days -= 181}
        213..=243 => {months = 8; days -= 212}
        244..=273 => {months = 9; days -= 243}
        274..=304 => {months = 10; days -= 273}
        305..=334 => {months = 11; days -= 304}
        335..=366 => {months = 12; days -= 334}
        _ => {months = 12;}
    }
    // if days <= 31 { months = 1;}
    // else if days > 31 && days <= 59 {months = 2; days -= 31}
    // else if days > 59 && days <= 90 {months = 3; days -= 59}
    // else if days > 90 && days <= 120 {months = 4; days -= 90}
    // else if days > 120 && days <= 151 {months = 5; days -= 120}
    // else if days > 151 && days <= 181 {months = 6; days -= 151}
    // else if days > 181 && days <= 212 {months = 7; days -= 181}
    // else if days > 212 && days <= 243 {months = 8; days -= 212}
    // else if days > 243 && days <= 273 {months = 9; days -= 243}
    // else if days > 273 && days <= 304 {months = 10; days -= 273}
    // else if days > 304 && days <= 334 {months = 11; days -= 304}
    // else if days > 334 && days <= 366 {months = 12; days -= 334}
    // else {months = 12;}

    let minutes: String;
    if (seconds%HOUR)/MINUTE < 10 {
        minutes = format!("0{}", (seconds%HOUR)/MINUTE);
    } else {
        minutes = format!("{}", (seconds%HOUR)/MINUTE);
    }

    format!("{}.{}.{} - {}:{}:{}", days, months, 1970 + seconds / YEAR, (seconds%DAY)/HOUR, minutes, seconds%MINUTE)
}


pub struct Logger {
    log: Vec<String>,
    path: String,
    pub log_size: usize,
    logs_made: u16,
    loghead: u64,
}


impl Logger {
    
    pub fn new(log_folder: String) -> Logger{
        Logger {
            log: Vec::new(),
            path: log_folder,
            log_size: 0,
            logs_made: 0,
            loghead: get_current_time(),
        }
    }

    pub fn log(&mut self, s: &str) {
        if s.len() > 1024 {
            self.log.push(format!("{}; {}\n", unix_time_to_real(get_current_time()), "Tried to log an entry bigger than 1024 bytes"));
            return;
        }
        self.log.push(format!("{}; {}\n", unix_time_to_real(get_current_time()), s));
        if size_of_val(self.log.as_slice()) > 1_000_000 {
            let log_path = format!("{}{}{}.txt", self.path, PATH_SEP, unix_time_to_real(self.loghead).replace(':', ";"));
            let mut printer = "".to_owned();
            for line in self.log.iter() {
                printer.push_str(line);
            }
            match fs::write(log_path, printer) {
                Ok(_) => self.logs_made += 1,
                Err(why) => panic!("{}", why),
            };
            self.log.clear();
        }
    }


}

impl Drop for Logger {
    fn drop(&mut self) {
        let log_path = format!("{}{}{}.txt", self.path, PATH_SEP, unix_time_to_real(self.loghead).replace(':', ";"));
        let log_path_clone = log_path.clone();
        let mut printer = "".to_owned();
        for line in self.log.iter() {
            printer.push_str(line);
        }
        match fs::write(log_path, printer) {
            Ok(_) => self.logs_made += 1,
            Err(why) => {
                println!("{}", log_path_clone);
                panic!("{}", why)
            },
        };
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_destructor() {
        let mut logger = Logger::new("testlogs".to_owned());
        logger.log("Here is a log");
        if true {
            let mut logg = Logger::new("testlogs1".to_owned());
            logg.log("Here is a different log");
        }
        println!("Here something is happening");
    }

    #[test]
    fn test_too_big_log() {
        let mut logger = Logger::new("testlogs".to_owned());
        let mut s = "".to_owned();
        let mut i = 0;
        while i < 2000 {
            s.push('a');
            i += 1;
        }
        logger.log(&s);
    }

    #[test]
    fn test_many_logs() {
        let mut i = 0;
        let mut logger = Logger::new("testlogs".to_owned());
        while i <1_000_000 {
            logger.log(&format!("Log nr. {}", i));
            i += 1;
        }
        println!("{}", logger.loghead);
    }

    #[test]
    fn test_timestamp() {
        println!("{}", get_current_time());
    }

    #[test]
    fn test_test() {
        println!("{}{}{}", "logs", PATH_SEP, "filename");
    }
}