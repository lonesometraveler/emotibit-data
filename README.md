# emotibit-data

![](https://img.shields.io/crates/v/emotibit-data.svg)
![](https://docs.rs/emotibit-data/badge.svg)
![](https://github.com/lonesometraveler/emotibit-data/workflows/build/badge.svg)

A Rust library for parsing raw EmotiBit data.

[EmotiBit](https://www.emotibit.com) is a wearable sensor module for capturing high-quality emotional, physiological, and movement data. Easy-to-use and scientifically-validated sensing lets you enjoy wireless data streaming to any platform or direct data recording to the built-in SD card.


## Documentation

* [API reference](https://docs.rs/emotibit-data/latest/emotibit_data/)

## Usage

Add the following line to your Cargo.toml file.

```
emotibit-data = "0.1"
```

## Examples

Transform a single CSV line to `DataPacket`.

```rust
use emotibit_data::types::DataPacket;

fn main() {
    let csv_str = "1126349,49106,10,PI,1,100,156593,156471,156372,156300,156205,156136,156130,156103,156051,156103";
    match TryInto::<DataPacket>::try_into(csv_str) {
        Ok(packet) => println!("{:?}", packet),
        Err(e) => println!("{:?}", e),
    }
}
```

Read a CSV file and populate `DataPacket`s.

```rust
use anyhow::Result;
use emotibit_data::{parser, types::DataPacket};

fn main() {
    let file_path = "raw_data.csv";
    let result: Vec<Result<DataPacket>> = parser::get_packets(file_path).unwrap();
    let (data_packets, errors): (Vec<_>, Vec<_>) = result.into_iter().partition(Result::is_ok);

    for packet in data_packets {
        println!("{:?}", packet.unwrap());
    }

    for error in errors {
        println!("{:?}", error);
    }
}
```

There are more examples in the [examples](https://github.com/lonesometraveler/emotibit-data/tree/main/examples) folder.
