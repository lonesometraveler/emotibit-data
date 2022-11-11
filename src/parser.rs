//! Parser functions
use crate::types::{DataPacket, DataType, TimeSync, TimeSyncMap};
use anyhow::{anyhow, Result};
use chrono::{offset::TimeZone, DateTime, Local, NaiveDateTime};
use csv::ReaderBuilder;
use itertools::izip;

/// Reads a csv file and creates `DataPacket`s
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

/// Finds blocks of RD, TL, and AK and creates `TimeSync`s
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

/// Creates a `TimeSyncMap`
pub fn generate_sync_map(packets: &[Result<DataPacket>]) -> Result<TimeSyncMap> {
    let filtered = packets
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|p| p.timestamp);

    let emotibit_start_time = filtered.clone().reduce(f64::min).unwrap();
    let emotibit_end_time = filtered.reduce(f64::max).unwrap();

    let syncs = find_syncs(packets)?;

    let quartiles: Vec<&[TimeSync]> = syncs
        .chunks(num::integer::div_ceil(syncs.len(), 4))
        .collect();

    let best_timestamps = match (
        shortest_round_trip(quartiles.get(0).unwrap()),
        shortest_round_trip(quartiles.get(1).unwrap()),
        shortest_round_trip(quartiles.get(2).unwrap()),
        shortest_round_trip(quartiles.get(3).unwrap()),
    ) {
        (Some(x), _, _, Some(y)) => Some((x, y)),
        (Some(x), _, Some(y), _) => Some((x, y)),
        (_, Some(x), _, Some(y)) => Some((x, y)),
        (_, Some(x), Some(y), _) => Some((x, y)),
        (Some(x), Some(y), _, _) => Some((x, y)),
        (_, _, Some(x), Some(y)) => Some((x, y)),
        // TODO: add data from the same quartile
        _ => None,
    };

    if let Some(best_timestamps) = best_timestamps {
        let (tl0, te0) = get_c_e(&best_timestamps.0);
        let (tl1, te1) = get_c_e(&best_timestamps.1);

        Ok(TimeSyncMap {
            te0,
            te1,
            tl0,
            tl1,
            syncs_received: syncs.len(),
            emotibit_start_time,
            emotibit_end_time,
            parse_version: "lonesomtraveler.0.0.1".to_owned(),
        })
    } else {
        Err(anyhow!("Cannot generate a time sync map"))
    }
}

fn shortest_round_trip(vec: &[TimeSync]) -> Option<TimeSync> {
    let shortest = vec.iter().map(|x| x.round_trip).reduce(f64::min).unwrap();
    for ts in vec.iter() {
        if ts.round_trip == shortest {
            return Some(ts.clone());
        }
    }
    None
}

// TODO: rename function
fn get_c_e(sync: &TimeSync) -> (f64, f64) {
    let e0 = sync.ts_received;
    let ts = &sync.ts_sent;

    let pos = ts.rfind('-').unwrap();
    let (head, tail) = ts.split_at(pos);

    let date_time =
        NaiveDateTime::parse_from_str(head, "%Y-%m-%d_%H-%M-%S").expect("Invalid format");
    let date_time: DateTime<Local> = Local.from_local_datetime(&date_time).unwrap();
    let c = date_time.timestamp();

    let last_n_char = tail.len() - 1;
    let m: f64 = tail[1..].parse().unwrap();
    let m = m / 10_i32.pow(last_n_char.try_into().unwrap()) as f64;

    let mut c0 = (c as f64) + m;
    c0 += sync.round_trip as f64 / 2_f64 / 1000_f64;
    (c0, e0)
}
