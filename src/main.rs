use parking_lot::RwLock;
use dashmap::DashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use bytes::{BytesMut, BufMut, Buf};
use std::sync::Arc;
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;
use std::fs;

// Helper function to get current time in milliseconds
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RedisValueType {
    String(String),
    List(VecDeque<String>),
    Integer(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisValue {
    data: RedisValueType,
    expiry: Option<u64>,
}

#[derive(Debug, Clone)]
enum RespData {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespData>),
    Null,
}

enum SetOptions {
    None,
    EX(u64),
    PX(u64),
    EXAT(u64),
    PXAT(u64),
}

struct RedisStore {
    data: DashMap<String, RedisValue>,
    next_cleanup: RwLock<u64>,
    cleanup_interval: u64,
}

impl RedisStore {
    fn new() -> Self {
        RedisStore {
            data: DashMap::new(),
            next_cleanup: RwLock::new(current_time_ms()),
            cleanup_interval: 100,
        }
    }

    fn get(&self, key: &str) -> Option<RedisValue> {
        if let Some(entry) = self.data.get(key) {
            if let Some(expiry) = entry.expiry {
                let now = current_time_ms();
                if now >= expiry {
                    self.data.remove(key);
                    return None;
                }
            }
            Some(entry.clone())
        } else {
            None
        }
    }

    fn set_with_options(&self, key: String, value: RedisValueType, options: SetOptions) {
        let expiry = match options {
            SetOptions::None => None,
            SetOptions::EX(seconds) => Some(current_time_ms() + seconds * 1000),
            SetOptions::PX(millis) => Some(current_time_ms() + millis),
            SetOptions::EXAT(timestamp) => Some(timestamp * 1000),
            SetOptions::PXAT(timestamp) => Some(timestamp),
        };
        
        self.data.insert(key, RedisValue { data: value, expiry });
    }

    fn exists(&self, key: &str) -> bool {
        if let Some(entry) = self.data.get(key) {
            if let Some(expiry) = entry.expiry {
                if current_time_ms() >= expiry {
                    self.data.remove(key);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    fn del(&self, keys: &[String]) -> usize {
        keys.iter().filter(|k| self.data.remove(*k).is_some()).count()
    }

    fn incr(&self, key: &str) -> Result<i64, String> {
        loop {
            match self.get(key) {
                Some(value) => {
                    match value.data {
                        RedisValueType::Integer(n) => {
                            let new_value = n + 1;
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::Integer(new_value),
                                SetOptions::None
                            );
                            return Ok(new_value);
                        }
                        RedisValueType::String(s) => {
                            match s.parse::<i64>() {
                                Ok(n) => {
                                    let new_value = n + 1;
                                    self.set_with_options(
                                        key.to_string(),
                                        RedisValueType::Integer(new_value),
                                        SetOptions::None
                                    );
                                    return Ok(new_value);
                                }
                                Err(_) => return Err("value is not an integer".to_string()),
                            }
                        }
                        _ => return Err("value is not an integer".to_string()),
                    }
                }
                None => {
                    self.set_with_options(
                        key.to_string(),
                        RedisValueType::Integer(1),
                        SetOptions::None
                    );
                    return Ok(1);
                }
            }
        }
    }

    fn decr(&self, key: &str) -> Result<i64, String> {
        loop {
            match self.get(key) {
                Some(value) => {
                    match value.data {
                        RedisValueType::Integer(n) => {
                            let new_value = n - 1;
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::Integer(new_value),
                                SetOptions::None
                            );
                            return Ok(new_value);
                        }
                        RedisValueType::String(s) => {
                            match s.parse::<i64>() {
                                Ok(n) => {
                                    let new_value = n - 1;
                                    self.set_with_options(
                                        key.to_string(),
                                        RedisValueType::Integer(new_value),
                                        SetOptions::None
                                    );
                                    return Ok(new_value);
                                }
                                Err(_) => return Err("value is not an integer".to_string()),
                            }
                        }
                        _ => return Err("value is not an integer".to_string()),
                    }
                }
                None => {
                    self.set_with_options(
                        key.to_string(),
                        RedisValueType::Integer(-1),
                        SetOptions::None
                    );
                    return Ok(-1);
                }
            }
        }
    }

    fn lpush(&self, key: &str, values: Vec<String>) -> usize {
        loop {
            match self.get(key) {
                Some(mut value) => {
                    match &mut value.data {
                        RedisValueType::List(list) => {
                            for v in values.iter().rev() {
                                list.push_front(v.clone());
                            }
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::List(list.clone()),
                                SetOptions::None
                            );
                            return list.len();
                        }
                        _ => {
                            let mut list = VecDeque::new();
                            for v in values.iter().rev() {
                                list.push_front(v.clone());
                            }
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::List(list.clone()),
                                SetOptions::None
                            );
                            return list.len();
                        }
                    }
                }
                None => {
                    let mut list = VecDeque::new();
                    for v in values.iter().rev() {
                        list.push_front(v.clone());
                    }
                    self.set_with_options(
                        key.to_string(),
                        RedisValueType::List(list.clone()),
                        SetOptions::None
                    );
                    return list.len();
                }
            }
        }
    }

    fn rpush(&self, key: &str, values: Vec<String>) -> usize {
        loop {
            match self.get(key) {
                Some(mut value) => {
                    match &mut value.data {
                        RedisValueType::List(list) => {
                            for v in values {
                                list.push_back(v);
                            }
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::List(list.clone()),
                                SetOptions::None
                            );
                            return list.len();
                        }
                        _ => {
                            let mut list = VecDeque::new();
                            for v in values {
                                list.push_back(v);
                            }
                            self.set_with_options(
                                key.to_string(),
                                RedisValueType::List(list.clone()),
                                SetOptions::None
                            );
                            return list.len();
                        }
                    }
                }
                None => {
                    let mut list = VecDeque::new();
                    for v in values {
                        list.push_back(v);
                    }
                    self.set_with_options(
                        key.to_string(),
                        RedisValueType::List(list.clone()),
                        SetOptions::None
                    );
                    return list.len();
                }
            }
        }
    }

    fn save(&self) -> std::io::Result<()> {
        let data: Vec<(String, RedisValue)> = self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        
        let serialized = serde_json::to_string(&data)?;
        fs::write("redis-data.json", serialized)?;
        Ok(())
    }

    fn load(&self) -> std::io::Result<()> {
        match fs::read_to_string("redis-data.json") {
            Ok(contents) => {
                let data: Vec<(String, RedisValue)> = serde_json::from_str(&contents)?;
                for (key, value) in data {
                    self.data.insert(key, value);
                }
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn maybe_cleanup(&self) {
        let now = current_time_ms();
        let mut next_cleanup = self.next_cleanup.write();
        if now < *next_cleanup {
            return;
        }
        *next_cleanup = now + self.cleanup_interval;

        let keys: Vec<String> = self.data.iter()
            .take(20)
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some(entry) = self.data.get(&key) {
                if let Some(expiry) = entry.expiry {
                    if now >= expiry {
                        self.data.remove(&key);
                    }
                }
            }
        }
    }
}

fn parse_resp(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if buffer.is_empty() {
        return Ok(None);
    }

    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '-' => parse_error(buffer),
        ':' => parse_integer(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(Error::new(ErrorKind::InvalidData, "Invalid RESP data type")),
    }
}

fn parse_simple_string(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if let Some(pos) = find_crlf(buffer, 1)? {
        let string = String::from_utf8_lossy(&buffer[1..pos]).to_string();
        Ok(Some((pos + 2, RespData::SimpleString(string))))
    } else {
        Ok(None)
    }
}

fn parse_error(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if let Some(pos) = find_crlf(buffer, 1)? {
        let string = String::from_utf8_lossy(&buffer[1..pos]).to_string();
        Ok(Some((pos + 2, RespData::Error(string))))
    } else {
        Ok(None)
    }
}

fn parse_integer(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if let Some(pos) = find_crlf(buffer, 1)? {
        let num_str = String::from_utf8_lossy(&buffer[1..pos]);
        match num_str.parse::<i64>() {
            Ok(num) => Ok(Some((pos + 2, RespData::Integer(num)))),
            Err(_) => Err(Error::new(ErrorKind::InvalidData, "Invalid integer")),
        }
    } else {
        Ok(None)
    }
}

fn parse_bulk_string(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if let Some(pos) = find_crlf(buffer, 1)? {
        let len_str = String::from_utf8_lossy(&buffer[1..pos]);
        let len: i64 = len_str.parse().map_err(|_| {
            Error::new(ErrorKind::InvalidData, "Invalid bulk string length")
        })?;

        if len == -1 {
            return Ok(Some((pos + 2, RespData::Null)));
        }

        let str_start = pos + 2;
        let str_end = str_start + len as usize;
        let total_end = str_end + 2;

        if buffer.len() >= total_end {
            let string = String::from_utf8_lossy(&buffer[str_start..str_end]).to_string();
            Ok(Some((total_end, RespData::BulkString(string))))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn parse_array(buffer: &mut BytesMut) -> std::io::Result<Option<(usize, RespData)>> {
    if let Some(pos) = find_crlf(buffer, 1)? {
        let len_str = String::from_utf8_lossy(&buffer[1..pos]);
        let len: i64 = len_str.parse().map_err(|_| {
            Error::new(ErrorKind::InvalidData, "Invalid array length")
        })?;

        if len == -1 {
            return Ok(Some((pos + 2, RespData::Null)));
        }

        let mut current_pos = pos + 2;
        let mut elements = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let mut temp_buffer = BytesMut::from(&buffer[current_pos..]);
            if let Some((consumed, element)) = parse_resp(&mut temp_buffer)? {
                elements.push(element);
                current_pos += consumed;
            }
            else {
                return Ok(None);
            }
        }

        Ok(Some((current_pos, RespData::Array(elements))))
    } else {
        Ok(None)
    }
}

fn find_crlf(buffer: &[u8], start: usize) -> std::io::Result<Option<usize>> {
    for i in start..buffer.len() - 1 {
        if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

fn serialize_resp(data: &RespData) -> Vec<u8> {
    let mut buffer = Vec::new();
    match data {
        RespData::SimpleString(s) => {
            buffer.extend_from_slice(b"+");
            buffer.extend_from_slice(s.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        RespData::Error(s) => {
            buffer.extend_from_slice(b"-");
            buffer.extend_from_slice(s.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        RespData::Integer(n) => {
            buffer.extend_from_slice(b":");
            buffer.extend_from_slice(n.to_string().as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        RespData::BulkString(s) => {
            buffer.extend_from_slice(b"$");
            buffer.extend_from_slice(s.len().to_string().as_bytes());
            buffer.extend_from_slice(b"\r\n");
            buffer.extend_from_slice(s.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        RespData::Array(arr) => {
            buffer.extend_from_slice(b"*");
            buffer.extend_from_slice(arr.len().to_string().as_bytes());
            buffer.extend_from_slice(b"\r\n");
            for item in arr {
                buffer.extend_from_slice(&serialize_resp(item));
            }
        }
        RespData::Null => {
            buffer.extend_from_slice(b"$-1\r\n");
        }
    }
    buffer
}

async fn handle_command(command: &RespData, store: &RedisStore) -> std::io::Result<RespData> {
    match command {
        RespData::Array(array) => {
            if let Some(RespData::BulkString(cmd)) = array.get(0) {
                match cmd.to_uppercase().as_str() {
                    "PING" => Ok(RespData::SimpleString("PONG".to_string())),
                    
                    "ECHO" => {
                        if let Some(arg) = array.get(1) {
                            Ok(arg.clone())
                        } else {
                            Ok(RespData::Error("ERR wrong number of arguments for 'echo' command".to_string()))
                        }
                    }
                    
                    "SET" => {
                        if array.len() < 3 {
                            return Ok(RespData::Error("ERR wrong number of arguments for 'set' command".to_string()));
                        }
                        
                        if let (Some(RespData::BulkString(key)), Some(RespData::BulkString(value))) = (array.get(1), array.get(2)) {
                            let mut options = SetOptions::None;
                            
                            // Handle SET options
                            if array.len() > 3 {
                                for i in (3..array.len()).step_by(2) {
                                    if let Some(RespData::BulkString(opt)) = array.get(i) {
                                        match opt.to_uppercase().as_str() {
                                            "EX" => {
                                                if let Some(RespData::BulkString(secs)) = array.get(i + 1) {
                                                    if let Ok(seconds) = secs.parse::<u64>() {
                                                        options = SetOptions::EX(seconds);
                                                    }
                                                }
                                            }
                                            "PX" => {
                                                if let Some(RespData::BulkString(ms)) = array.get(i + 1) {
                                                    if let Ok(millis) = ms.parse::<u64>() {
                                                        options = SetOptions::PX(millis);
                                                    }
                                                }
                                            }
                                            // Add other options as needed
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            
                            store.set_with_options(key.clone(), RedisValueType::String(value.clone()), options);
                            Ok(RespData::SimpleString("OK".to_string()))
                        } else {
                            Ok(RespData::Error("ERR invalid arguments for 'set' command".to_string()))
                        }
                    }
                    
                    "GET" => {
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            match store.get(key) {
                                Some(value) => match value.data {
                                    RedisValueType::String(s) => Ok(RespData::BulkString(s)),
                                    RedisValueType::Integer(n) => Ok(RespData::BulkString(n.to_string())),
                                    _ => Ok(RespData::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())),
                                },
                                None => Ok(RespData::Null),
                            }
                        } else {
                            Ok(RespData::Error("ERR wrong number of arguments for 'get' command".to_string()))
                        }
                    }
                    
                    "EXISTS" => {
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            Ok(RespData::Integer(if store.exists(key) { 1 } else { 0 }))
                        } else {
                            Ok(RespData::Error("ERR wrong number of arguments for 'exists' command".to_string()))
                        }
                    }
                    
                    "DEL" => {
                        let keys: Vec<String> = array[1..].iter()
                            .filter_map(|x| match x {
                                RespData::BulkString(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect();
                        Ok(RespData::Integer(store.del(&keys) as i64))
                    }
                    
                    "INCR" => {
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            match store.incr(key) {
                                Ok(n) => Ok(RespData::Integer(n)),
                                Err(e) => Ok(RespData::Error(e)),
                            }
                        } else {
                            Ok(RespData::Error("ERR wrong number of arguments for 'incr' command".to_string()))
                        }
                    }
                    
                    "DECR" => {
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            match store.decr(key) {
                                Ok(n) => Ok(RespData::Integer(n)),
                                Err(e) => Ok(RespData::Error(e)),
                            }
                        } else {
                            Ok(RespData::Error("ERR wrong number of arguments for 'decr' command".to_string()))
                        }
                    }
                    
                    "LPUSH" => {
                        if array.len() < 3 {
                            return Ok(RespData::Error("ERR wrong number of arguments for 'lpush' command".to_string()));
                        }
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            let values: Vec<String> = array[2..].iter()
                                .filter_map(|x| match x {
                                    RespData::BulkString(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .collect();
                            Ok(RespData::Integer(store.lpush(key, values) as i64))
                        } else {
                            Ok(RespData::Error("ERR invalid arguments for 'lpush' command".to_string()))
                        }
                    }
                    
                    "RPUSH" => {
                        if array.len() < 3 {
                            return Ok(RespData::Error("ERR wrong number of arguments for 'rpush' command".to_string()));
                        }
                        if let Some(RespData::BulkString(key)) = array.get(1) {
                            let values: Vec<String> = array[2..].iter()
                                .filter_map(|x| match x {
                                    RespData::BulkString(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .collect();
                            Ok(RespData::Integer(store.rpush(key, values) as i64))
                        } else {
                            Ok(RespData::Error("ERR invalid arguments for 'rpush' command".to_string()))
                        }
                    }
                    
                    "SAVE" => {
                        match store.save() {
                            Ok(_) => Ok(RespData::SimpleString("OK".to_string())),
                            Err(e) => Ok(RespData::Error(format!("ERR {}", e))),
                        }
                    }
                    
                    _ => Ok(RespData::Error("ERR unknown command".to_string())),
                }
            } else {
                Ok(RespData::Error("ERR invalid command format".to_string()))
            }
        }
        _ => Ok(RespData::Error("ERR invalid command format".to_string())),
    }
}

async fn handle_connection(stream: TcpStream, store: Arc<RedisStore>) -> std::io::Result<()> {
    let (mut reader, writer) = tokio::io::split(stream);
    let mut writer = BufWriter::new(writer);
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read data into buffer
        let n = reader.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }

        // Parse and handle commands
        while let Some((consumed, command)) = parse_resp(&mut buffer)? {
            buffer.advance(consumed); // This now works because we imported Buf trait
            let response = handle_command(&command, &store).await?;
            writer.write_all(&serialize_resp(&response)).await?;
            writer.flush().await?;
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Redis server listening on port 6379...");

    let store = Arc::new(RedisStore::new());
    
    // Load existing data if any
    if let Err(e) = store.load() {
        eprintln!("Error loading data: {}", e);
    }

    loop {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        
        // Create a new clone for the cleanup operation
        let cleanup_store = Arc::clone(&store);
        
        // Create another clone for the connection handler
        let connection_store = Arc::clone(&store);
        
        tokio::spawn(async move {
            if let Err(err) = handle_connection(socket, connection_store).await {
                eprintln!("Error handling connection: {}", err);
            }
        });
        
        // Use the cleanup store clone
        cleanup_store.maybe_cleanup();
    }
}