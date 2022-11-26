use anyhow::Result;
use emotibit_data::{parser, types::DataPacket};
use std::path::PathBuf;

fn main() {
    match export(Some(PathBuf::from("raw_data.csv"))) {
        Ok(_) => println!("successs"),
        Err(e) => println!("{:?}", e),
    }
}

fn export(path_buf: Option<PathBuf>) -> Result<()> {
    let packets: Vec<DataPacket> = parser::get_packets(
        &path_buf
            .as_ref()
            .unwrap()
            .clone()
            .into_os_string()
            .into_string()
            .unwrap(),
    )?
    .into_iter()
    .filter_map(|x| x.ok())
    .collect();

    let mut file = std::fs::File::create("postcard.bin")?;
    for packet in packets {
        let mut buf = [0u8; 512];
        match postcard::to_slice_cobs(&packet, &mut buf) {
            Ok(d) => {
                std::io::Write::write(&mut file, d)?;
            }
            Err(e) => println!("unexpected error: {:?}", e),
        }
    }

    Ok(())
}
