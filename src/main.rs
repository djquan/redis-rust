use std::collections::VecDeque;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::parser::RespType;

mod parser;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listen_and_serve(listener);
}

fn listen_and_serve(listener: TcpListener) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    loop {
                        handle_response(&stream);
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_response(stream: &TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    let parsed = parser::parse(&mut reader);

    let mut commands = match parsed {
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
                    _ => {}
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

    fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
        let addr = listener.local_addr().expect("Failed to get local address");

        thread::spawn(move || { listen_and_serve(listener); });

        return addr;
    }
}