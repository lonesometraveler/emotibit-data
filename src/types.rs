//! Types for this crate
use anyhow::{anyhow, Result};
use csv::StringRecord;
use itertools::Itertools;
use std::str::FromStr;

/// Returns CSV values
pub trait Csv {
    fn csv(&self) -> Vec<StringRecord>;
}

impl Csv for StringRecord {
    fn csv(&self) -> Vec<StringRecord> {
        vec![self.clone()]
    }
}

/// Emotibit Data Packet
#[derive(Debug, Clone)]
pub struct DataPacket {
    /// Local timestamp on a host PC
    pub host_timestamp: Option<f64>,
    /// Milliseconds since start of EmotiBit
    pub emotibit_timestamp: f64,
    /// Packet count since start of EmotiBit
    pub packet_id: u32,
    /// Number of data points in the payload
    pub data_points: u8,
    /// Version of packet protocol
    pub version: u8,
    /// Data reliability score out of 100, currently always 100
    pub reliability: u8,
    /// Type of data being sent and its payload
    pub data_type: DataType,
}

impl Csv for DataPacket {
    fn csv(&self) -> Vec<StringRecord> {
        let mut vec = Vec::new();
        let payload = Self::parse_data_type(&self.data_type, self.data_type.payload());
        for p in payload {
            let host_timestamp = match self.host_timestamp {
                Some(n) => n.to_string(),
                None => "NaN".to_owned(),
            };
            vec.push(StringRecord::from(vec![
                host_timestamp,
                self.emotibit_timestamp.to_string(),
                self.packet_id.to_string(),
                self.data_points.to_string(),
                self.data_type.as_str().to_owned(),
                self.version.to_string(),
                self.reliability.to_string(),
                p.to_string(),
            ]));
        }
        vec
    }
}

impl DataPacket {
    fn parse_data_type(data_type: &DataType, payload: Vec<String>) -> Vec<String> {
        use DataType::*;
        match data_type {
            TxLcLm(_) | TxTlLc(_) => vec![format!(
                "{},{}",
                payload.get(0).unwrap(),
                payload.get(1).unwrap()
            )],
            _ => payload,
        }
    }
    /// Performs linear interpolation based on `TimeSyncMap` and returns a new `DataPacket` with a host timestamp.
    pub fn inject_host_timestamp(self, map: &TimeSyncMap) -> Self {
        let timestamp = map.tl0
            + (map.tl1 - map.tl0) * (self.emotibit_timestamp - map.te0) / (map.te1 - map.te0);
        DataPacket {
            host_timestamp: Some(timestamp),
            emotibit_timestamp: self.emotibit_timestamp,
            packet_id: self.packet_id,
            data_points: self.data_points,
            version: self.version,
            reliability: self.reliability,
            data_type: self.data_type,
        }
    }
}

impl TryFrom<&StringRecord> for DataPacket {
    type Error = anyhow::Error;
    fn try_from(r: &StringRecord) -> Result<Self, Self::Error> {
        if let (
            Some(timestamp),
            Some(packet_id),
            Some(data_points),
            Some(data_type),
            Some(version),
            Some(reliability),
        ) = (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5))
        {
            Ok(DataPacket {
                host_timestamp: None,
                emotibit_timestamp: timestamp.parse()?,
                packet_id: packet_id.parse()?,
                data_points: data_points.parse()?,
                version: version.parse()?,
                reliability: reliability.parse()?,
                data_type: get_data_type(r, data_type)?,
            })
        } else {
            Err(anyhow!("Missing Column, record: {:?}", r))
        }
    }
}

impl TryFrom<&str> for DataPacket {
    type Error = anyhow::Error;
    fn try_from(str: &str) -> Result<Self, Self::Error> {
        let r = &csv::StringRecord::from(str.split(',').collect::<Vec<_>>());
        r.try_into()
    }
}

#[test]
fn string_to_data() {
    let input = "1126349,49106,10,PI,1,100,156593,156471,156372,156300,156205,156136,156130,156103,156051,156103";
    let packet: DataPacket = input.try_into().unwrap();
    assert_eq!(packet.packet_id, 49106);
    assert_eq!(packet.data_points, 10);
}

/// Emotibit data type
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// EDA- Electrodermal Activity
    EA(Vec<f32>),
    /// EDL- Electrodermal Level
    EL(Vec<f32>),
    /// EDR- Electrodermal Response (EmotiBit V4+ combines ER into EA signal)
    ER(Vec<f32>),
    /// PPG Infrared
    PI(Vec<u32>),
    /// PPG Red
    PR(Vec<u32>),
    /// PPG Green
    PG(Vec<u32>),
    /// Temperature 0
    T0(Vec<f32>),
    /// Temperature 1
    T1(Vec<f32>),
    /// Temperature via Medical-grade Thermopile (only on EmotiBit MD)
    TH(Vec<f32>),
    /// Accelerometer X
    AX(Vec<f32>),
    /// Accelerometer Y
    AY(Vec<f32>),
    /// Accelerometer Z
    AZ(Vec<f32>),
    /// Gyroscope X
    GX(Vec<f32>),
    /// Gyroscope Y
    GY(Vec<f32>),
    /// Gyroscope Z
    GZ(Vec<f32>),
    /// Magnetometer X
    MX(Vec<i32>),
    /// Magnetometer Y
    MY(Vec<i32>),
    /// Magnetometer Z
    MZ(Vec<i32>),
    /// Battery Voltage
    BV(Vec<f32>),
    /// Battery Percentage Remaining (B%)
    BATLV(Vec<u32>),
    AK(Vec<String>),
    /// Request Data, TypeTag in Payload
    RD(Vec<String>),
    TL(String),
    TX(Vec<String>),
    TxTlLc((String, f32)),
    TxLcLm(Vec<f32>),
    EM(Vec<String>),
    /// Heart Rate
    HR(Vec<i32>),
    /// Heart Inter-beat Interval
    BI(Vec<i32>),
    /// Skin Conductance Response (SCR) Amplitude
    SA(Vec<f32>),
    /// Skin Conductance Response (SCR) Frequency
    SF(Vec<f32>),
    /// Skin Conductance Response (SCR) Rise Time
    SR(Vec<f32>),
    /// User Note
    UN(Vec<String>),
    /// LSL Marker/message
    LM(String),
    /// Record begin (Include timestamp in Data)
    RB(String),
}

impl DataType {
    pub fn as_str(&self) -> &'static str {
        use DataType::*;
        match self {
            EA(_) => "EA",
            EL(_) => "EL",
            ER(_) => "ER",
            PI(_) => "PI",
            PR(_) => "PR",
            PG(_) => "PG",
            T0(_) => "T0",
            T1(_) => "T1",
            TH(_) => "TH",
            AX(_) => "AX",
            AY(_) => "AY",
            AZ(_) => "AZ",
            GX(_) => "GX",
            GY(_) => "GY",
            GZ(_) => "GZ",
            MX(_) => "MX",
            MY(_) => "MY",
            MZ(_) => "MZ",
            BV(_) => "BV",
            BATLV(_) => "B%",
            AK(_) => "AK",
            RD(_) => "RD",
            TL(_) => "TL",
            TX(_) => "TX",
            TxTlLc(_) => "TX_TL_LC",
            TxLcLm(_) => "TX_LC_LM",
            EM(_) => "EM",
            HR(_) => "HR",
            BI(_) => "BI",
            SA(_) => "SA",
            SF(_) => "SF",
            SR(_) => "SR",
            // Computer data TypeTags (sent over reliable channel e.g. Control)
            UN(_) => "UN",
            LM(_) => "LM",
            // Control TypeTags
            RB(_) => "RB",
        }
    }

    pub fn payload(&self) -> Vec<String> {
        use DataType::*;
        match self {
            EA(v) => v.iter().map(|p| p.to_string()).collect(),
            EL(v) => v.iter().map(|p| p.to_string()).collect(),
            ER(v) => v.iter().map(|p| p.to_string()).collect(),
            PI(v) => v.iter().map(|p| p.to_string()).collect(),
            PR(v) => v.iter().map(|p| p.to_string()).collect(),
            PG(v) => v.iter().map(|p| p.to_string()).collect(),
            T0(v) => v.iter().map(|p| p.to_string()).collect(),
            T1(v) => v.iter().map(|p| p.to_string()).collect(),
            TH(v) => v.iter().map(|p| p.to_string()).collect(),
            AX(v) => v.iter().map(|p| p.to_string()).collect(),
            AY(v) => v.iter().map(|p| p.to_string()).collect(),
            AZ(v) => v.iter().map(|p| p.to_string()).collect(),
            GX(v) => v.iter().map(|p| p.to_string()).collect(),
            GY(v) => v.iter().map(|p| p.to_string()).collect(),
            GZ(v) => v.iter().map(|p| p.to_string()).collect(),
            MX(v) => v.iter().map(|p| p.to_string()).collect(),
            MY(v) => v.iter().map(|p| p.to_string()).collect(),
            MZ(v) => v.iter().map(|p| p.to_string()).collect(),
            BV(v) => v.iter().map(|p| p.to_string()).collect(),
            BATLV(v) => v.iter().map(|p| p.to_string()).collect(),
            AK(sv) => sv.to_vec(),
            RD(sv) => sv.to_vec(),
            TL(s) => vec![s.to_owned()],
            TX(sv) => sv.to_vec(),
            TxTlLc((s, f)) => vec![s.to_owned(), f.to_string()],
            TxLcLm(v) => v.iter().map(|p| p.to_string()).collect(),
            EM(sv) => sv.to_vec(),
            HR(v) => v.iter().map(|p| p.to_string()).collect(),
            BI(v) => v.iter().map(|p| p.to_string()).collect(),
            SA(v) => v.iter().map(|p| p.to_string()).collect(),
            SF(v) => v.iter().map(|p| p.to_string()).collect(),
            SR(v) => v.iter().map(|p| p.to_string()).collect(),
            // // Computer data TypeTags (sent over reliable channel e.g. Control)
            UN(sv) => sv.to_vec(),
            LM(s) => vec![s.to_owned()],
            // // Control TypeTags
            RB(s) => vec![s.to_owned()],
        }
    }
}

fn get_data_type(record: &StringRecord, type_str: &str) -> Result<DataType> {
    let skip_to_payload = 6_usize;
    match type_str {
        "RB" => Ok(DataType::RB(to_string(record, skip_to_payload))),
        "AK" => Ok(DataType::AK(to_string_vec(record, skip_to_payload))),
        "EA" => Ok(DataType::EA(to_vec::<f32>(record, skip_to_payload)?)),
        "EL" => Ok(DataType::EL(to_vec::<f32>(record, skip_to_payload)?)),
        "ER" => Ok(DataType::ER(to_vec::<f32>(record, skip_to_payload)?)),
        "PI" => Ok(DataType::PI(to_vec::<u32>(record, skip_to_payload)?)),
        "PR" => Ok(DataType::PR(to_vec::<u32>(record, skip_to_payload)?)),
        "PG" => Ok(DataType::PG(to_vec::<u32>(record, skip_to_payload)?)),
        "T0" => Ok(DataType::T0(to_vec::<f32>(record, skip_to_payload)?)),
        "T1" => Ok(DataType::T1(to_vec::<f32>(record, skip_to_payload)?)),
        "TH" => Ok(DataType::TH(to_vec::<f32>(record, skip_to_payload)?)),
        "AX" => Ok(DataType::AX(to_vec::<f32>(record, skip_to_payload)?)),
        "AY" => Ok(DataType::AY(to_vec::<f32>(record, skip_to_payload)?)),
        "AZ" => Ok(DataType::AZ(to_vec::<f32>(record, skip_to_payload)?)),
        "GX" => Ok(DataType::GX(to_vec::<f32>(record, skip_to_payload)?)),
        "GY" => Ok(DataType::GY(to_vec::<f32>(record, skip_to_payload)?)),
        "GZ" => Ok(DataType::GZ(to_vec::<f32>(record, skip_to_payload)?)),
        "MX" => Ok(DataType::MX(to_vec::<i32>(record, skip_to_payload)?)),
        "MY" => Ok(DataType::MY(to_vec::<i32>(record, skip_to_payload)?)),
        "MZ" => Ok(DataType::MZ(to_vec::<i32>(record, skip_to_payload)?)),
        "BI" => Ok(DataType::BI(to_vec::<i32>(record, skip_to_payload)?)),
        "SA" => Ok(DataType::SA(to_vec::<f32>(record, skip_to_payload)?)),
        "SF" => Ok(DataType::SF(to_vec::<f32>(record, skip_to_payload)?)),
        "SR" => Ok(DataType::SR(to_vec::<f32>(record, skip_to_payload)?)),
        "BV" => Ok(DataType::BV(to_vec::<f32>(record, skip_to_payload)?)),
        "HR" => Ok(DataType::HR(to_vec::<i32>(record, skip_to_payload)?)),
        "B%" => Ok(DataType::BATLV(to_vec::<u32>(record, skip_to_payload)?)),
        "RD" => Ok(DataType::RD(to_string_vec(record, skip_to_payload))),
        "UN" => Ok(DataType::UN(to_string_vec(record, skip_to_payload))),
        "EM" => Ok(DataType::EM(to_string_vec(record, skip_to_payload))),
        "TL" => Ok(DataType::TL(to_string(record, skip_to_payload))),
        "TX" => Ok(DataType::TX(to_string_vec(record, skip_to_payload))),
        "LM" => Ok(DataType::LM(to_string(record, skip_to_payload))),
        // TODO: add data types
        _ => Err(anyhow!("Unknown Type: {}, {:?}", type_str, record)),
    }
}

// Helpers
fn to_string(record: &StringRecord, index: usize) -> String {
    record.iter().skip(index).join(",")
}

fn to_vec<T>(record: &StringRecord, index_from: usize) -> Result<Vec<T>>
where
    T: num::Num + FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let mut errors = vec![];
    let vec = record
        .iter()
        .skip(index_from)
        .map(|x| x.to_string().trim().parse::<T>())
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .collect::<Vec<T>>();
    if errors.is_empty() {
        Ok(vec)
    } else {
        Err(anyhow!("Parse to Num Error: {:?}", record))
    }
}

fn to_string_vec(record: &StringRecord, index_from: usize) -> Vec<String> {
    record
        .iter()
        .skip(index_from)
        .map(|str| str.to_owned())
        .collect()
}

/// Time Syncs
#[derive(Debug, Clone)]
pub struct TimeSync {
    /// Emotibit local time when RD was sent
    pub rd: f64,
    /// Emotibit local time when TS was received
    pub ts_received: f64,
    /// Timestamp at the moment TS was sent in `%Y-%m-%d_%H-%M-%S_f` format
    pub ts_sent: String,
    /// Emotibit local time when AK was sent
    pub ak: f64,
    /// Duration for the round trip Emotibit -> PC -> Emotibit
    pub round_trip: f64,
}

impl Csv for TimeSync {
    fn csv(&self) -> Vec<StringRecord> {
        vec![StringRecord::from(vec![
            self.rd.to_string(),
            self.ts_received.to_string(),
            self.ts_sent.to_owned(),
            self.ak.to_string(),
            self.round_trip.to_string(),
        ])]
    }
}

/// Time Sync Map
#[derive(Debug)]
pub struct TimeSyncMap {
    pub te0: f64,
    pub te1: f64,
    pub tl0: f64,
    pub tl1: f64,
    pub syncs_received: usize,
    pub emotibit_start_time: f64,
    pub emotibit_end_time: f64,
    pub parse_version: String,
}

impl Csv for TimeSyncMap {
    fn csv(&self) -> Vec<StringRecord> {
        vec![StringRecord::from(vec![
            self.te0.to_string(),
            self.te1.to_string(),
            self.tl0.to_string(),
            self.tl1.to_string(),
            self.syncs_received.to_string(),
            self.emotibit_start_time.to_string(),
            self.emotibit_end_time.to_string(),
            self.parse_version.to_owned(),
        ])]
    }
}
