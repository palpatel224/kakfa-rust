#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Write;

struct KafkaMessage {
    message_size : i32,
    header : KafkaHeader,
}
struct KafkaHeader{
    correlation_id : i32,
}

impl Into<Vec<u8>> for KafkaMessage{
    fn into(self) -> Vec<u8>{
        let mut bytes = vec![];
        bytes.extend_from_slice(&self.message_size.to_be_bytes());
        bytes.extend_from_slice(&self.header.correlation_id.to_be_bytes());
        bytes
    }
}
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let response = KafkaMessage{
                    message_size: 4,
                    header : KafkaHeader { correlation_id:7 }
                };
                let buf: Vec<u8> = response.into();
                stream.write(buf.as_slice()).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
