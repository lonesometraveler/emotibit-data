# emotibit-data

![](https://img.shields.io/crates/v/emotibit-data.svg)
![](https://docs.rs/emotibit-data/badge.svg)
![](https://github.com/lonesometraveler/emotibit-data/workflows/build/badge.svg)

A Rust library for parsing raw EmotiBit data.

[Emotibit](https://www.emotibit.com) is a wearable sensor module for capturing high-quality emotional, physiological, and movement data. Easy-to-use and scientifically-validated sensing lets you enjoy wireless data streaming to any platform or direct data recording to the built-in SD card.


## Document

* [API reference](https://docs.rs/crate/emotibit-data)

## Examples

Basic example programs are under `examples` folder.

### to_csv.rs

This reads a csv file, parses data, and outputs processed data in csv files just like Emotibit's official DataParser does. Place a raw data file named raw_data.csv in the root directory and run this. 

```
cargo run --example to_csv 
```

### heart_rate.rs

This reads a csv file and extracts heart rate data (tagged HR). It then calculates the average heart rate. Place a raw data file named raw_data.csv in the root directory.

```
cargo run --example heart_rate 
```

### udp_server.rs

This starts a UDP server and parses udp packets to Rust data. The server expects data in emotibit csv format. (example: "1126349,49106,10,PI,1,100,156593,156471,156372,156300,156205,156136,156130,156103,156051,156103")

```
cargo run --release --example udp_server --features tokio
```
