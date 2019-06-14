mod protos;
mod reader;
mod writer;
mod schema;

pub use writer::{Writer, Config};

#[cfg(test)]
mod tests {
    use super::*;
    use super::schema::*;
    use std::io::Result;
    use std::fs::File;
    use crate::writer::{Data, LongData};

    #[test]
    fn writer() -> Result<()> {
        let schema = Schema::Long;
        let mut out = File::create("target/test.orc")?;
        // let mut v: Vec<u8> = vec![];
        let config = Config::default();
        let mut writer = Writer::new(&mut out, &schema, &config)?;
        let mut data = writer.data();
        if let Data::Long(data) = data {
            for i in 0..10 {
                data.write(Some(i));
            }
        } else { unreachable!(); }
        writer.write_batch(10)?;
        writer.finish()?;
        Ok(())
    }
}
