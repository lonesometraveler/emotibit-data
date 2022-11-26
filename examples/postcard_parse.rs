use anyhow::Result;
use emotibit_data::types::DataPacket;
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn main() {
    match message_from_binary("postcard.bin") {
        Ok(result) => println!("successs: len: {}", result.len()),
        Err(e) => println!("{:?}", e),
    }
}

fn message_from_binary<T: AsRef<Path>>(path: T) -> Result<Vec<DataPacket>> {
    let mut buf = Vec::<u8>::new();
    let mut f = File::open(path)?;
    f.read_to_end(&mut buf)?;

    Ok(buf
        .split(|x| x == &0)
        .map(|x| x.to_owned())
        .flat_map(|mut v| postcard::from_bytes_cobs::<DataPacket>(&mut v))
        .collect())
}
