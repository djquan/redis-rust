use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
    let message = "+PONG\r\n";
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    // iterate over stream and use the lines() method to get an iterator
    println!("accepted new connection");
    for line_result in reader.lines() {
        let _line = match line_result {
            Ok(line) => line,
            Err(e) => {
                println!("error: {}", e);
                return;
            }
        };

        writer.write_all(message.as_bytes()).unwrap();
        writer.flush().unwrap();
    }
}
