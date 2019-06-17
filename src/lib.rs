
mod buffer;
mod protos;
pub mod reader;
pub mod schema;
pub mod writer;

#[cfg(test)]
mod tests {
    use super::schema::*;
    use super::*;
    use crate::writer::{Config, Data, LongData, Writer};
    use std::fs::File;
    use std::io::Result;
    
    #[test]
    fn writer() -> Result<()> {
        let schema = Schema::Long;
        let mut out = File::create("target/test.orc")?;
        let config = Config::default();
        let mut writer = Writer::new(&mut out, &schema, &config)?;
        let data = writer.data();
        if let Data::Long(data) = data {
            for i in 0..10 {
                data.write(Some(i));
            }
        } else {
            unreachable!();
        }
        writer.write_batch(10)?;
        writer.finish()?;
        Ok(())
    }
}
