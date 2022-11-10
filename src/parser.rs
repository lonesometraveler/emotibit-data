use crate::types::{DataPacket, DataType, TimeSync};
use anyhow::{anyhow, Result};
use csv::ReaderBuilder;
use itertools::izip;

pub fn get_packets(file_path: &str) -> Result<Vec<Result<DataPacket>>> {
    let mut vec: Vec<Result<DataPacket>> = Vec::new();

    for record in ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_path(file_path)?
        .records()
    {
        match DataPacket::try_from(record.as_ref().unwrap()) {
            Ok(packet) => match split_tx_or_as_is(packet) {
                Ok(packet) => vec.push(Ok(packet)),
                Err(e) => vec.push(Err(anyhow!("{}, record: {:?}", e, record))),
            },
            Err(e) => vec.push(Err(anyhow!("{}, record: {:?}", e, record))),
        }
    }

    Ok(vec)
}

fn split_tx_or_as_is(x: DataPacket) -> Result<DataPacket> {
    if let DataType::TX(data) = x.clone().data_type {
        if let (Some(tag1), Some(val1), Some(tag2), Some(val2)) =
            (data.get(0), data.get(1), data.get(2), data.get(3))
        {
            let data_type = match (tag1.as_ref(), tag2.as_ref()) {
                ("LC", "LM") => Some(DataType::TxLcLm(vec![val1.parse()?, val2.parse()?])),
                ("TL", "LC") => Some(DataType::TxTlLc((val1.to_owned(), val2.parse()?))),
                _ => None,
            };

            if let Some(data_type) = data_type {
                return Ok(DataPacket {
                    timestamp: x.timestamp,
                    packet_id: x.packet_id,
                    data_points: x.data_points,
                    version: x.version,
                    reliability: x.reliability,
                    data_type,
                });
            }
            return Err(anyhow!("Invalid data"));
        }
    }
    Ok(x)
}

// Find a block of RD, TL, and AK
pub fn find_syncs(packets: &[Result<DataPacket>]) -> Result<Vec<TimeSync>> {
    use DataType::*;
    let mut vec = vec![];
    let syncs: Vec<&DataPacket> = packets
        .iter()
        .filter_map(|x| {
            x.as_ref().ok().and_then(|x| match x.data_type {
                RD(_) | TL(_) | AK(_) => Some(x),
                _ => None,
            })
        })
        .collect();

    let syncs2 = syncs.clone();
    let syncs3 = syncs.clone();
    for (rd, tl, ak) in izip!(&syncs, &syncs2[1..], &syncs3[2..]) {
        if let (RD(_), TL(date_time), AK(_)) = (&rd.data_type, &tl.data_type, &ak.data_type) {
            vec.push(TimeSync {
                rd: rd.timestamp,
                ts_received: tl.timestamp,
                ts_sent: date_time.to_owned(),
                ak: ak.timestamp,
                round_trip: tl.timestamp - rd.timestamp,
            });
        }
    }
    Ok(vec)
}
