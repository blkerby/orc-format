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

    #[test]
    fn writer() -> Result<()> {
        let schema = Schema::Long;
        let mut out = File::create("target/test.orc")?;
        // let mut v: Vec<u8> = vec![];
        let config = Config::default();
        let writer = Writer::new(&mut out, &schema, &config)?;
        writer.finish()?;
        println!("{:?}", out);
        Ok(())
    }
}
