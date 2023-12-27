use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listen_and_serve(listener);
}

fn listen_and_serve(listener: TcpListener) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_response(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_response(stream: TcpStream) {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(line) => line.trim().to_uppercase(),
            Err(e) => {
                println!("error: {}", e);
                return;
            }
        };

        match line.trim() {
            "PING" => {
                writer.write_all(b"+PONG\r\n").unwrap();
                writer.flush().unwrap();
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
        stream.shutdown(std::net::Shutdown::Both).unwrap();

        assert_eq!(received, "+PONG\r\n");
    }

    fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
        let addr = listener.local_addr().expect("Failed to get local address");

        thread::spawn(move || { listen_and_serve(listener); });

        return addr;
    }
}