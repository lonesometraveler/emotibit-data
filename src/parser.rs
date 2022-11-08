use crate::types::DataPacket;
use anyhow::{anyhow, Result};
use csv::ReaderBuilder;

pub fn get_packets(file_path: &str) -> Result<Vec<Result<DataPacket>>> {
    let mut vec: Vec<Result<DataPacket>> = Vec::new();

    for r in ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_path(file_path)?
        .records()
    {
        match r.as_ref() {
            Ok(r) => match DataPacket::try_from(r) {
                Ok(p) => vec.push(Ok(p)),
                Err(e) => vec.push(Err(anyhow!(
                    "Packet Conversion Error: {:?}, record: {:?}",
                    e,
                    r
                ))),
            },
            Err(e) => vec.push(Err(anyhow!("StringRecord: {:?}, record: {:?}", e, r))),
        }
    }

    Ok(vec)
}
