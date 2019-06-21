use orc_rs::schema::{Schema, Field};
use orc_rs::writer::SnappyCompression;
use orc_rs::writer::{Config, Writer, Data};
use std::fs::File;
use std::io::Result;

fn main() -> Result<()> {
    let schema = Schema::Struct(vec![
        Field("x".to_owned(), Schema::Long), 
        Field("y".to_owned(), Schema::Long),
        Field("z".to_owned(), Schema::String),
        Field("a".to_owned(), Schema::Double),
    ]);
    // let schema = Schema::Long;
    // let mut out = File::create("/dev/null")?;
    let mut out = File::create("target/test.orc")?;
    let config = Config::new().with_compression(SnappyCompression::new().build());
    let mut writer = Writer::new(&mut out, &schema, &config)?;
    let batch_size: i64 = 10;
    for n in 0..1 {
    // for n in 0..100000 {
        let data = writer.data();        
        if let Data::Struct(struct_data) = data {
            let children = struct_data.children();
            if let Data::Long(long_data) = &mut children[0] {
                long_data.write(None);
                for j in 0..batch_size - 1 {
                    long_data.write(Some(n * batch_size + j));
                }
            } else { unreachable!() }
            if let Data::Long(long_data) = &mut children[1] {
                for j in 0..batch_size - 1 {
                    long_data.write(Some(n * batch_size + j * j));
                }
                long_data.write(None);
            } else { unreachable!() }
            if let Data::String(string_data) = &mut children[2] {
                for j in 0..batch_size {
                    let s = format!("hello {}", j);
                    string_data.write(Some(&s));
                }
            } else { unreachable!() }
            if let Data::Double(double_data) = &mut children[3] {
                for j in 0..batch_size {
                    double_data.write(Some((j as f64) * 0.01));
                }
            } else { unreachable!() }


            for _ in 0..batch_size {
                struct_data.write(true);
            }
        } else { unreachable!() }
        writer.write_batch(batch_size as u64)?;
        
        let data = writer.data();
        if let Data::Struct(struct_data) = data {
            struct_data.write(false);
        }
        writer.write_batch(1)?;
    }
    writer.finish()?;
    Ok(())
}
