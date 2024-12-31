use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

mod resp;
use resp::{deserialize, serialize, RespType};

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    while let Ok(size) = stream.read(&mut buffer) {
        if size == 0 {
            break; // Connection closed by client
        }

        let input = &buffer[..size];
        let input_str = String::from_utf8_lossy(input);

        match deserialize(&mut input_str.chars().peekable()) {
            Ok(command) => {
                let response = match command {
                    RespType::Array(elements) => {
                        if let Some(RespType::SimpleString(cmd)) = elements.get(0) {
                            match cmd.to_uppercase().as_str() {
                                "PING" => serialize(&RespType::SimpleString("PONG".into())),
                                "ECHO" => {
                                    if let Some(arg) = elements.get(1) {
                                        serialize(arg)
                                    } else {
                                        serialize(&RespType::Error("ECHO requires an argument".into()))
                                    }
                                }
                                _ => serialize(&RespType::Error(format!("Unknown command: {}", cmd))),
                            }
                        } else {
                            serialize(&RespType::Error("Invalid command format".into()))
                        }
                    }
                    _ => serialize(&RespType::Error("Expected an array of commands".into())),
                };

                // Send the response to the client
                if let Ok(response) = response {
                    stream.write_all(response.as_bytes()).unwrap();
                }
            }
            Err(e) => {
                let error_response = serialize(&RespType::Error(format!("Error: {}", e))).unwrap();
                stream.write_all(error_response.as_bytes()).unwrap();
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6380").expect("Failed to bind to port 6380");
    println!("Redis Lite server is running on port 6380");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a thread to handle the connection
                thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}
