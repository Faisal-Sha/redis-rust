use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, Error, ErrorKind};
use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// RESP Protocol Data Types
#[derive(Debug, Clone)]
enum RespData {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RespData>>),
}

// Redis Value Storage
#[derive(Debug, Clone)]
struct RedisValue {
    data: String,
    expiry: Option<u64>,  // Unix timestamp for expiration
}

struct RedisStore {
    data: HashMap<String, RedisValue>,
}

impl RedisStore {
    fn new() -> Self {
        RedisStore {
            data: HashMap::new(),
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.data.get(key) {
            if let Some(expiry) = value.expiry {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if now > expiry {
                    return None;
                }
            }
            Some(value.data.clone())
        } else {
            None
        }
    }

    fn set(&mut self, key: String, value: String, expiry: Option<u64>) {
        self.data.insert(key, RedisValue { data: value, expiry });
    }

    fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }
}

// RESP Protocol Parser Functions
fn parse_resp(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::WouldBlock, "Incomplete data"));
    }

    match input[0] as char {
        '+' => parse_simple_string(&input[1..]),
        '-' => parse_error(&input[1..]),
        ':' => parse_integer(&input[1..]),
        '$' => parse_bulk_string(&input[1..]),
        '*' => parse_array(&input[1..]),
        _ => Err(Error::new(ErrorKind::InvalidData, "Invalid RESP data type")),
    }
}

fn parse_simple_string(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    let mut line = Vec::new();
    let mut i = 0;
    
    while i < input.len() {
        if input[i] == b'\r' && i + 1 < input.len() && input[i + 1] == b'\n' {
            let string = String::from_utf8(line)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            return Ok((input[i + 2..].to_vec(), RespData::SimpleString(string)));
        }
        line.push(input[i]);
        i += 1;
    }
    
    Err(Error::new(ErrorKind::WouldBlock, "Incomplete simple string"))
}

fn parse_error(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    let (remainder, data) = parse_simple_string(input)?;
    if let RespData::SimpleString(string) = data {
        Ok((remainder, RespData::Error(string)))
    } else {
        Err(Error::new(ErrorKind::InvalidData, "Expected simple string"))
    }
}

fn parse_integer(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    let (remainder, data) = parse_simple_string(input)?;
    if let RespData::SimpleString(string) = data {
        let number = string.parse::<i64>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        Ok((remainder, RespData::Integer(number)))
    } else {
        Err(Error::new(ErrorKind::InvalidData, "Expected simple string"))
    }
}

fn parse_bulk_string(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    let (mut remainder, data) = parse_simple_string(input)?;
    let length_str = if let RespData::SimpleString(s) = data {
        s
    } else {
        return Err(Error::new(ErrorKind::InvalidData, "Expected simple string"));
    };
    
    let length = length_str.parse::<i64>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    
    if length < 0 {
        return Ok((remainder, RespData::BulkString(None)));
    }
    
    let length = length as usize;
    if remainder.len() < length + 2 {
        return Err(Error::new(ErrorKind::WouldBlock, "Incomplete bulk string"));
    }
    
    let string = String::from_utf8(remainder[..length].to_vec())
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    
    remainder = remainder[length + 2..].to_vec(); // +2 for CRLF
    Ok((remainder, RespData::BulkString(Some(string))))
}

fn parse_array(input: &[u8]) -> std::io::Result<(Vec<u8>, RespData)> {
    let (mut remainder, data) = parse_simple_string(input)?;
    let length_str = if let RespData::SimpleString(s) = data {
        s
    } else {
        return Err(Error::new(ErrorKind::InvalidData, "Expected simple string"));
    };
    
    let length = length_str.parse::<i64>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    
    if length < 0 {
        return Ok((remainder, RespData::Array(None)));
    }
    
    let mut elements = Vec::new();
    for _ in 0..length {
        let (new_remainder, element) = parse_resp(&remainder)?;
        remainder = new_remainder;
        elements.push(element);
    }
    
    Ok((remainder, RespData::Array(Some(elements))))
}

fn write_resp(stream: &mut TcpStream, data: &RespData) -> std::io::Result<()> {
    let response = match data {
        RespData::SimpleString(s) => format!("+{}\r\n", s),
        RespData::Error(s) => format!("-{}\r\n", s),
        RespData::Integer(i) => format!(":{}\r\n", i),
        RespData::BulkString(None) => "$-1\r\n".to_string(),
        RespData::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
        RespData::Array(None) => "*-1\r\n".to_string(),
        RespData::Array(Some(arr)) => {
            let mut result = format!("*{}\r\n", arr.len());
            for element in arr {
                result.push_str(&format_resp(element));
            }
            result
        }
    };
    
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn format_resp(data: &RespData) -> String {
    match data {
        RespData::SimpleString(s) => format!("+{}\r\n", s),
        RespData::Error(s) => format!("-{}\r\n", s),
        RespData::Integer(i) => format!(":{}\r\n", i),
        RespData::BulkString(None) => "$-1\r\n".to_string(),
        RespData::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
        RespData::Array(None) => "*-1\r\n".to_string(),
        RespData::Array(Some(arr)) => {
            let mut result = format!("*{}\r\n", arr.len());
            for element in arr {
                result.push_str(&format_resp(element));
            }
            result
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    println!("Redis server listening on port 6379...");

    let store = Arc::new(Mutex::new(RedisStore::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store_clone = Arc::clone(&store);
                thread::spawn(move || {
                    handle_connection(stream, store_clone).unwrap_or_else(|error| {
                        eprintln!("Error handling connection: {}", error);
                    });
                });
            }
            Err(e) => eprintln!("Error accepting connection: {}", e),
        }
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<RedisStore>>) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut temp_buffer = [0; 512];

    loop {
        let n = stream.read(&mut temp_buffer)?;
        if n == 0 {
            return Ok(());
        }
        
        buffer.extend_from_slice(&temp_buffer[..n]);
        
        match parse_resp(&buffer) {
            Ok((remainder, command)) => {
                buffer = remainder;
                let response = handle_command(&command, &store)?;
                write_resp(&mut stream, &response)?;
            }
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    write_resp(&mut stream, &RespData::Error(format!("Parse error: {}", e)))?;
                    buffer.clear();
                }
            }
        }
    }
}

fn handle_command(command: &RespData, store: &Arc<Mutex<RedisStore>>) -> std::io::Result<RespData> {
    let array = match command {
        RespData::Array(Some(arr)) => arr,
        _ => return Ok(RespData::Error("ERR invalid command format".to_string())),
    };

    if array.is_empty() {
        return Ok(RespData::Error("ERR empty command".to_string()));
    }

    let command_name = match &array[0] {
        RespData::BulkString(Some(s)) => s.to_uppercase(),
        _ => return Ok(RespData::Error("ERR invalid command name".to_string())),
    };

    match command_name.as_str() {
        "PING" => Ok(RespData::SimpleString("PONG".to_string())),
        
        "GET" => {
            if array.len() != 2 {
                return Ok(RespData::Error("ERR wrong number of arguments for 'get' command".to_string()));
            }
            let key = match &array[1] {
                RespData::BulkString(Some(s)) => s,
                _ => return Ok(RespData::Error("ERR invalid key".to_string())),
            };
            
            let store = store.lock().unwrap();
            match store.get(key) {
                Some(value) => Ok(RespData::BulkString(Some(value))),
                None => Ok(RespData::BulkString(None)),
            }
        }

        "SET" => {
            if array.len() < 3 {
                return Ok(RespData::Error("ERR wrong number of arguments for 'set' command".to_string()));
            }
            
            let key = match &array[1] {
                RespData::BulkString(Some(s)) => s.clone(),
                _ => return Ok(RespData::Error("ERR invalid key".to_string())),
            };
            
            let value = match &array[2] {
                RespData::BulkString(Some(s)) => s.clone(),
                _ => return Ok(RespData::Error("ERR invalid value".to_string())),
            };

            let mut expiry = None;
            if array.len() > 3 {
                if array.len() < 5 {
                    return Ok(RespData::Error("ERR syntax error".to_string()));
                }
                
                match &array[3] {
                    RespData::BulkString(Some(s)) if s.to_uppercase() == "EX" => {
                        if let RespData::BulkString(Some(seconds)) = &array[4] {
                            if let Ok(secs) = seconds.parse::<u64>() {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                expiry = Some(now + secs);
                            }
                        }
                    }
                    _ => return Ok(RespData::Error("ERR syntax error".to_string())),
                }
            }

            let mut store = store.lock().unwrap();
            store.set(key, value, expiry);
            Ok(RespData::SimpleString("OK".to_string()))
        }

        "DEL" => {
            if array.len() < 2 {
                return Ok(RespData::Error("ERR wrong number of arguments for 'del' command".to_string()));
            }
            
            let mut count = 0;
            let mut store = store.lock().unwrap();
            
            for key_data in &array[1..] {
                if let RespData::BulkString(Some(key)) = key_data {
                    if store.del(key) {
                        count += 1;
                    }
                }
            }
            
            Ok(RespData::Integer(count))
        }

        "EXISTS" => {
            if array.len() < 2 {
                return Ok(RespData::Error("ERR wrong number of arguments for 'exists' command".to_string()));
            }
            
            let mut count = 0;
            let store = store.lock().unwrap();
            
            for key_data in &array[1..] {
                if let RespData::BulkString(Some(key)) = key_data {
                    if store.exists(key) {
                        count += 1;
                    }
                }
            }
            
            Ok(RespData::Integer(count))
        }

        _ => Ok(RespData::Error(format!("ERR unknown command '{}'", command_name))),
    }
}