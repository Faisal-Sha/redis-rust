use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, PartialEq)]
pub enum RespType {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Null,
    Array(Vec<RespType>),
}

pub fn deserialize(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    match chars.peek() {
        Some('+') => parse_simple_string(chars),
        Some('-') => parse_error(chars),
        Some(':') => parse_integer(chars),
        Some('$') => parse_bulk_string(chars),
        Some('*') => parse_array(chars),
        _ => Err("Invalid RESP format".to_string()),
    }
}

// In resp.rs

pub fn serialize(resp: &RespType) -> Result<String, String> {
    match resp {
        RespType::SimpleString(s) => Ok(format!("+{}\r\n", s)),
        RespType::Error(s) => Ok(format!("-{}\r\n", s)),
        RespType::Integer(i) => Ok(format!(":{}\r\n", i)),
        RespType::BulkString(s) => Ok(format!("${}\r\n{}\r\n", s.len(), s)),
        RespType::Null => Ok("$-1\r\n".to_string()),
        RespType::Array(arr) => {
            let mut result = format!("*{}\r\n", arr.len());
            for item in arr {
                result.push_str(&serialize(item)?);
            }
            Ok(result)
        },
    }
}


fn parse_simple_string(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    chars.next(); // Consume '+'
    let line = parse_line(chars)?;
    Ok(RespType::SimpleString(line))
}

fn parse_error(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    chars.next(); // Consume '-'
    let line = parse_line(chars)?;
    Ok(RespType::Error(line))
}

fn parse_integer(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    chars.next(); // Consume ':'
    let line = parse_line(chars)?;
    let value = line.parse::<i64>().map_err(|_| "Invalid integer".to_string())?;
    Ok(RespType::Integer(value))
}

fn parse_bulk_string(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    chars.next(); // Consume '$'
    let line = parse_line(chars)?;
    if line == "-1" {
        return Ok(RespType::Null);
    }
    let len = line.parse::<usize>().map_err(|_| "Invalid bulk string length".to_string())?;
    let mut value = String::new();
    for _ in 0..len {
        if let Some(c) = chars.next() {
            value.push(c);
        } else {
            return Err("Unexpected end of input".to_string());
        }
    }
    consume_crlf(chars)?;
    Ok(RespType::BulkString(value))
}

fn parse_array(chars: &mut Peekable<Chars>) -> Result<RespType, String> {
    chars.next(); // Consume '*'
    let line = parse_line(chars)?;
    if line == "-1" {
        return Ok(RespType::Null);
    }
    let len = line.parse::<usize>().map_err(|_| "Invalid array length".to_string())?;
    let mut elements = Vec::new();
    for _ in 0..len {
        elements.push(deserialize(chars)?);
    }
    Ok(RespType::Array(elements))
}

fn parse_line(chars: &mut Peekable<Chars>) -> Result<String, String> {
    let mut line = String::new();
    while let Some(&c) = chars.peek() {
        if c == '\r' {
            chars.next(); // Consume '\r'
            if chars.next() == Some('\n') {
                break;
            } else {
                return Err("Invalid line ending".to_string());
            }
        }
        line.push(c);
        chars.next();
    }
    Ok(line)
}

fn consume_crlf(chars: &mut Peekable<Chars>) -> Result<(), String> {
    if chars.next() == Some('\r') && chars.next() == Some('\n') {
        Ok(())
    } else {
        Err("Expected CRLF".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_peekable(input: &str) -> Peekable<Chars> {
        input.chars().peekable()
    }

    #[test]
    fn test_simple_string() {
        let mut chars = to_peekable("+OK\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::SimpleString("OK".into()));
    }

    #[test]
    fn test_error() {
        let mut chars = to_peekable("-Error message\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::Error("Error message".into()));
    }

    #[test]
    fn test_integer() {
        let mut chars = to_peekable(":1000\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::Integer(1000));
    }

    #[test]
    fn test_bulk_string() {
        let mut chars = to_peekable("$6\r\nfoobar\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::BulkString("foobar".into()));
    }

    #[test]
    fn test_null_bulk_string() {
        let mut chars = to_peekable("$-1\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::Null);
    }

    #[test]
    fn test_array() {
        let mut chars = to_peekable("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n");
        assert_eq!(
            deserialize(&mut chars).unwrap(),
            RespType::Array(vec![
                RespType::BulkString("get".into()),
                RespType::BulkString("key".into())
            ])
        );
    }

    #[test]
    fn test_null_array() {
        let mut chars = to_peekable("*-1\r\n");
        assert_eq!(deserialize(&mut chars).unwrap(), RespType::Null);
    }

    #[test]
    fn test_invalid_input() {
        let mut chars = to_peekable("invalid");
        assert!(deserialize(&mut chars).is_err());
    }
}
