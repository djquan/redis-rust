use std::io::{BufRead, BufReader, Read};

use crate::parser::RespType::{Array, BulkString};

// create a type for a Redis Serialization Protocol Data class

#[derive(PartialEq, Debug)]
pub enum RespType {
    BulkString(String, Vec<u8>),
    Array(Vec<RespType>, Vec<u8>),
}

pub(crate) fn parse<T: Read>(buf_reader: &mut BufReader<T>) -> RespType {
    // read one byte from buf_reader
    let mut buf = [0; 1];
    buf_reader.read_exact(&mut buf).expect("Expect to read a byte");

    match buf[0] {
        b'*' => parse_array(buf_reader),
        b'$' => parse_bulk_string(buf_reader),
        _ => panic!("Unexpected byte"),
    }
}

fn parse_bulk_string<T: Read>(reader: &mut BufReader<T>) -> RespType {
    let mut bytes = Vec::new();
    reader.read_until(b'\n', &mut bytes).expect("Expect to read a header line");
    let length = String::from_utf8_lossy(&bytes[0..bytes.len() - 2]);
    let length_conv = length.parse::<usize>().expect("Expect to parse a length");

    let mut data = vec![0; length_conv];
    reader.read_exact(&mut data).expect("Expect to read data");
    reader.read_exact(&mut [0; 2]).expect("Expect to read CRLF");

    return BulkString(String::from_utf8_lossy(&data).to_string(), vec![]);
}

fn parse_array<T: Read>(reader: &mut BufReader<T>) -> RespType {
    let mut result = Vec::new();

    let mut bytes = Vec::new();
    reader.read_until(b'\n', &mut bytes).expect("Expect to read a header line");
    let length = String::from_utf8_lossy(&bytes[0..bytes.len() - 2]);
    let length_conv = length.parse::<usize>().expect("Expect to parse a length");

    for _ in 0..length_conv {
        result.push(parse(reader));
    }

    return Array(result, vec![]);
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use crate::parser::{parse, RespType};

    #[test]
    fn it_works() {
        let mut reader = BufReader::new("*1\r\n$4\r\nPING\r\n".as_bytes());
        let result = parse(&mut reader);

        let expected = vec![RespType::BulkString("PING".to_string(), vec![])];

        assert_eq!(result, RespType::Array(expected, vec![]));
    }

    #[test]
    fn it_works_with_multiple_values() {
        let mut reader = BufReader::new("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n".as_bytes());
        let result = parse(&mut reader);

        let expected = vec![RespType::BulkString(
            "PING".to_string(),
            vec![],
        ), RespType::BulkString(
            "PONG".to_string(),
            vec![],
        )];

        assert_eq!(result, RespType::Array(expected, vec![]));
    }
}