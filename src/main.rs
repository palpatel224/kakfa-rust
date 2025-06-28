#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;

struct KafkaMessage {
    message_size: i32,
    header: KafkaHeader,
}
struct KafkaHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
}

impl Into<Vec<u8>> for KafkaMessage {
    fn into(self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.correlation_id.to_be_bytes());
        bytes
    }
}

fn read_request_header(
    stream: &mut std::net::TcpStream,
) -> Result<KafkaHeader, Box<dyn std::error::Error>> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let message_size = i32::from_be_bytes(size_buf);

    println!("Message size: {}", message_size);

    let mut api_key_buf = [0u8; 2];
    stream.read_exact(&mut api_key_buf)?;
    let request_api_key = i16::from_be_bytes(api_key_buf);

    let mut api_version_buf = [0u8; 2];
    stream.read_exact(&mut api_version_buf)?;
    let request_api_version = i16::from_be_bytes(api_version_buf);

    let mut correlation_buf = [0u8; 4];
    stream.read_exact(&mut correlation_buf)?;
    let correlation_id = i32::from_be_bytes(correlation_buf);

    println!("Extracted correlation_id: {}", correlation_id);

    let remaining_bytes = (message_size - 8) as usize;
    if remaining_bytes > 0 {
        let mut remaining_buf = vec![0u8; remaining_bytes];
        stream.read_exact(&mut remaining_buf)?;
        println!("Read {} remaining bytes", remaining_bytes);
    }

    Ok(KafkaHeader {
        request_api_key,
        request_api_version,
        correlation_id,
        client_id: String::new(),
    })
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                match read_request_header(&mut stream) {
                    Ok(request_header) => {
                        let response = KafkaMessage {
                            message_size: 4,
                            header: KafkaHeader {
                                request_api_key: 0,
                                request_api_version: 0,
                                correlation_id: request_header.correlation_id,
                                client_id: String::new(),
                            },
                        };
                        let buf: Vec<u8> = response.into();
                        stream.write_all(buf.as_slice()).unwrap();
                    }
                    Err(e) => {
                        println!("Error reading request: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
