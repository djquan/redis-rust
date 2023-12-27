use std::collections::{HashMap, VecDeque};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::parser::RespType;

mod parser;

struct DbEntry {
    value: String,
    ttl: u128,
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listen_and_serve(listener);
}

fn listen_and_serve(listener: TcpListener) {
    let db: Mutex<HashMap<String, DbEntry>> = Mutex::new(HashMap::new());
    let counter = Arc::new(db);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let thread_db = Arc::clone(&counter);
                thread::spawn(move || {
                    loop {
                        handle_response(&stream, &thread_db);
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_response(stream: &TcpStream, counter: &Arc<Mutex<HashMap<String, DbEntry>>>) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    let parsed = parser::parse(&mut reader);

    let mut commands = match parsed {
        RespType::EOF() => {
            return;
        }

        RespType::BulkString(_, _) => {
            panic!("First response should be an array")
        }

        RespType::Array(commands, _) => {
            VecDeque::from(commands)
        }
    };

    while commands.len() > 0 {
        let command = commands.pop_front().unwrap();
        match command {
            RespType::BulkString(command, _) => {
                match command.to_uppercase().as_str() {
                    "PING" => {
                        writer.write_all(b"+PONG\r\n").unwrap();
                        writer.flush().unwrap();
                    }
                    "ECHO" => {
                        let echo = commands.pop_front().unwrap();
                        match echo {
                            RespType::BulkString(echo, _) => {
                                writer.write_all(format!("${}\r\n{}\r\n", echo.len(), echo).as_bytes()).unwrap();
                                writer.flush().unwrap();
                            }
                            _ => {}
                        }
                    }
                    "SET" => {
                        let key = match commands.pop_front().unwrap() {
                            RespType::BulkString(key, _) => {
                                key
                            }
                            _ => {
                                panic!("Expected a bulk string")
                            }
                        };
                        let value = match commands.pop_front().unwrap() {
                            RespType::BulkString(value, _) => {
                                value
                            }
                            _ => {
                                panic!("Expected a bulk string")
                            }
                        };

                        let ttl = if commands.len() > 0 {
                            match commands.pop_front().unwrap() {
                                RespType::BulkString(s, bytes) => {
                                    if s.to_uppercase() == "PX" {
                                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
                                        match commands.pop_front().unwrap() {
                                            RespType::BulkString(s, bytes) => {
                                                s.parse::<u128>().unwrap() + now
                                            }
                                            _ => {
                                                panic!("Expected a bulk string")
                                            }
                                        }
                                    } else {
                                        commands.push_front(RespType::BulkString(s, bytes));
                                        0
                                    }
                                }
                                command => {
                                    commands.push_front(command);
                                    0
                                }
                            }
                        } else {
                            0
                        };

                        let mut db = counter.lock().unwrap();
                        db.insert(key, DbEntry {
                            value,
                            ttl,
                        });

                        writer.write_all(b"+OK\r\n").unwrap();
                        writer.flush().unwrap();
                    }
                    "GET" => {
                        let key = match commands.pop_front().unwrap() {
                            RespType::BulkString(key, _) => {
                                key
                            }
                            _ => {
                                panic!("Expected a bulk string")
                            }
                        };
                        let mut db = counter.lock().unwrap();

                        match db.get(&key) {
                            None => {
                                writer.write_all(b"$-1\r\n").unwrap();
                            }
                            Some(entry) => {
                                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();

                                if entry.ttl != 0 && entry.ttl < now {
                                    db.remove(&key);
                                    writer.write_all(b"$-1\r\n").unwrap();
                                } else {
                                    writer.write_all(format!("${}\r\n{}\r\n", entry.value.len(), entry.value).as_bytes()).unwrap();
                                }
                            }
                        }
                        writer.flush().unwrap();
                    }
                    _ => {
                        return;
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;

    use crate::listen_and_serve;

    #[test]
    fn ping_works() {
        let addr = start_server();

        let mut stream = TcpStream::connect(addr).expect("Failed to connect to server");
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();

        let mut buffer = [0; 1024];
        let n_bytes = stream.read(&mut buffer).expect("Failed to read from stream");
        let received = String::from_utf8_lossy(&buffer[..n_bytes]);

        assert_eq!(received, "+PONG\r\n");
    }

    #[test]
    fn echo_works() {
        let addr = start_server();

        let mut stream = TcpStream::connect(addr).expect("Failed to connect to server");
        stream.write_all(b"*2\r\n$4\r\nECHO\r\n$4\r\nHIHI\r\n").unwrap();

        let mut buffer = [0; 1024];
        let n_bytes = stream.read(&mut buffer).expect("Failed to read from stream");
        let received = String::from_utf8_lossy(&buffer[..n_bytes]);

        assert_eq!(received, "$4\r\nHIHI\r\n");
    }

    #[test]
    fn get_set_works() {
        let addr = start_server();

        let mut stream = TcpStream::connect(addr).expect("Failed to connect to server");
        stream.write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nHI\r\n$3\r\nBYE\r\n").unwrap();

        let mut buffer = [0; 1024];
        let n_bytes = stream.read(&mut buffer).expect("Failed to read from stream");
        let received = String::from_utf8_lossy(&buffer[..n_bytes]);

        assert_eq!(received, "+OK\r\n");

        let mut stream = TcpStream::connect(addr).expect("Failed to connect to server");
        stream.write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nHI\r\n").unwrap();

        let mut buffer = [0; 1024];
        let n_bytes = stream.read(&mut buffer).expect("Failed to read from stream");
        let received = String::from_utf8_lossy(&buffer[..n_bytes]);

        assert_eq!(received, "$3\r\nBYE\r\n");

        let mut stream = TcpStream::connect(addr).expect("Failed to connect to server");
        stream.write_all(b"*2\r\n$3\r\nGET\r\n$2\r\nBYE\r\n").unwrap();

        let mut buffer = [0; 1024];
        let n_bytes = stream.read(&mut buffer).expect("Failed to read from stream");
        let received = String::from_utf8_lossy(&buffer[..n_bytes]);

        assert_eq!(received, "$-1\r\n");
    }

    fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
        let addr = listener.local_addr().expect("Failed to get local address");

        thread::spawn(move || { listen_and_serve(listener); });

        return addr;
    }
}