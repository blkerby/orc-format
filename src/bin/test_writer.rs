use orc_rs::schema::{Schema, Field};
use orc_rs::writer::SnappyCompression;
use orc_rs::writer::{Config, Writer};
use std::fs::File;
use std::io::Result;

fn main() -> Result<()> {
    let schema = Schema::Struct(vec![
        Field("x".to_owned(), Schema::Long), 
        Field("y".to_owned(), Schema::Long),
        Field("z".to_owned(), Schema::String),
        Field("a".to_owned(), Schema::Double),
        Field("b".to_owned(), Schema::Float),
        Field("c".to_owned(), Schema::Date),
        Field("d".to_owned(), Schema::Boolean),
        Field("e".to_owned(), Schema::Decimal(15, 2)),
        Field("f".to_owned(), Schema::List(Box::new(Schema::Long))),
        Field("g".to_owned(), Schema::Map(Box::new(Schema::String), Box::new(Schema::Boolean))),
        Field("h".to_owned(), Schema::Timestamp),
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
        let root = data.unwrap_struct();
        let x = root.child(0).unwrap_long();
        x.write(None);
        for j in 0..batch_size - 1 {
            x.write(Some(n * batch_size + j));
        }
        let y = root.child(1).unwrap_long();
        for j in 0..batch_size - 1 {
            y.write(Some(n * batch_size + j * j));
        }
        y.write(None);
        let z = root.child(2).unwrap_string();
        for j in 0..batch_size {
            let s = format!("hello {}", j / 3);
            z.write(Some(&s));
        }
        let a = root.child(3).unwrap_double();
        for j in 0..batch_size {
            a.write(Some(((j / 3) as f64) * 0.01));
        }
        let b = root.child(4).unwrap_float();
        for j in 0..batch_size {
            b.write(Some(((j / 3) as f32) * 0.5));
        }
        let c = root.child(5).unwrap_long();
        for j in 0..batch_size {
            c.write(Some(j));
        }
        let d = root.child(6).unwrap_boolean();
        for j in 0..batch_size {
            d.write(Some((j % 3 == 0) as bool));
        }
        let e = root.child(7).unwrap_decimal64();
        for j in 0..batch_size {
            e.write(Some(j - batch_size / 2));
        }
        let f = root.child(8).unwrap_list();
        let f1 = f.child().unwrap_long();
        for j in 0..(batch_size * (batch_size - 1) / 2) {
            f1.write(Some(j));
        }
        for j in 0..batch_size {
            f.write(Some(j as u64));
        }
        
        let g = root.child(9).unwrap_map();
        let (gkey, gval) = g.children();
        let gkey_s = gkey.unwrap_string();
        let gval_b = gval.unwrap_boolean();
        for _ in 0..batch_size {
            gkey_s.write(Some("param"));
            gkey_s.write(Some("setting"));
        }
        for j in 0..batch_size {
            gval_b.write(Some((j % 2) != 0));
            gval_b.write(Some(((j + 1) % 2) != 0));
        }
        for _ in 0..batch_size {
            g.write(Some(2));
        }

        let h = root.child(10).unwrap_timestamp();
        for j in 0..batch_size {
            h.write_nanos(j, 10u32.pow((j % 9) as u32));
        }

        for _ in 0..batch_size {
            root.write(true);
        }
        writer.write_batch(batch_size as u64)?;
        
        let data = writer.data();
        let root = data.unwrap_struct();
        root.write(false);
        writer.write_batch(1)?;
    }
    writer.finish()?;
    Ok(())
}
