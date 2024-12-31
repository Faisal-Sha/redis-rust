use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

mod resp;
use resp::{deserialize, serialize, RespType};

type SharedDb = Arc<Mutex<HashMap<String, String>>>;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    println!("Redis Lite server listening on port 6379...");

    let db = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || handle_client(stream, db));
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, db: SharedDb) {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;

    loop {
        let mut buffer = String::new();
        if reader.read_line(&mut buffer).is_err() || buffer.is_empty() {
            break;
        }

        let mut chars = buffer.chars().peekable();
        match deserialize(&mut chars) {
            Ok(resp) => {
                let response = match resp {
                    RespType::Array(Some(args)) => handle_command(args, &db),
                    _ => RespType::Error("Invalid command".to_string()),
                };

                if let Ok(serialized) = serialize(response) {
                    if writer.write_all(serialized.as_bytes()).is_err() {
                        break;
                    }
                }
            }
            Err(err) => {
                if writer
                    .write_all(format!("-Error: {}\r\n", err).as_bytes())
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

fn handle_command(args: Vec<RespType>, db: &SharedDb) -> RespType {
    if args.is_empty() {
        return RespType::Error("Empty command".to_string());
    }

    match &args[0] {
        RespType::SimpleString(cmd) | RespType::BulkString(cmd) => match cmd.to_uppercase().as_str()
        {
            "SET" => {
                if args.len() != 3 {
                    return RespType::Error("SET command requires 2 arguments".to_string());
                }

                if let (Some(key), Some(value)) = (get_string(&args[1]), get_string(&args[2])) {
                    db.lock().unwrap().insert(key, value);
                    RespType::SimpleString("OK".to_string())
                } else {
                    RespType::Error("Invalid SET arguments".to_string())
                }
            }
            "GET" => {
                if args.len() != 2 {
                    return RespType::Error("GET command requires 1 argument".to_string());
                }

                if let Some(key) = get_string(&args[1]) {
                    match db.lock().unwrap().get(&key) {
                        Some(value) => RespType::BulkString(value.clone()),
                        None => RespType::Null,
                    }
                } else {
                    RespType::Error("Invalid GET argument".to_string())
                }
            }
            _ => RespType::Error("Unknown command".to_string()),
        },
        _ => RespType::Error("Invalid command".to_string()),
    }
}

fn get_string(resp: &RespType) -> Option<String> {
    match resp {
        RespType::SimpleString(s) | RespType::BulkString(s) => Some(s.clone()),
        _ => None,
    }
}
