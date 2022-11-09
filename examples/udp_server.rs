use emotibit_data::types::DataPacket;
use std::{env, str};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let socket = UdpSocket::bind(addr).await?;
    println!("Listening on: {}", socket.local_addr()?);

    let mut buf = [0u8; 1023];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, peer)) => {
                let str = str::from_utf8(&buf[..size]).unwrap().trim();
                match TryInto::<DataPacket>::try_into(str) {
                    Ok(packet) => println!("{:?}", packet),
                    Err(e) => println!("{:?}", e),
                }
            }
            Err(e) => println!("{:?}", e),
        }
    }
}
