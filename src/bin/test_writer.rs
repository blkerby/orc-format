use orc_format::schema::{Schema, Field};
use orc_format::writer::SnappyCompression;
use orc_format::writer::{GenericData, Config, Writer};
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
        Field("i".to_owned(), Schema::Union(vec![Schema::Long, Schema::Float])),
        Field("k".to_owned(), Schema::Binary),
    ]);
    // let schema = Schema::Long;
    // let mut out = File::create("/dev/null")?;
    let mut out = File::create("target/test.orc")?;
    let config = Config::new().with_compression(SnappyCompression::new().build());
    let mut writer = Writer::new(&mut out, &schema, config)?;
    let batch_size: i64 = 10;
    for n in 0..1 {
    // for n in 0..100000 {
        let data = writer.data();
        let root = data.unwrap_struct();
        let x = root.child(0).unwrap_long();
        x.write_null();
        for j in 0..batch_size - 1 {
            x.write(n * batch_size + j);
        }
        let y = root.child(1).unwrap_long();
        for j in 0..batch_size - 1 {
            y.write(n * batch_size + j * j);
        }
        y.write_null();
        let z = root.child(2).unwrap_string();
        for j in 0..batch_size {
            let s = format!("hello {}", j / 3);
            z.write(&s);
        }
        let a = root.child(3).unwrap_double();
        for j in 0..batch_size {
            a.write(((j / 3) as f64) * 0.01);
        }
        let b = root.child(4).unwrap_float();
        for j in 0..batch_size {
            b.write(((j / 3) as f32) * 0.5);
        }
        let c = root.child(5).unwrap_long();
        for j in 0..batch_size {
            c.write(j);
        }
        let d = root.child(6).unwrap_boolean();
        for j in 0..batch_size {
            d.write((j % 3 == 0) as bool);
        }
        let e = root.child(7).unwrap_decimal64();
        for j in 0..batch_size {
            e.write(j - batch_size / 2);
        }
        let f = root.child(8).unwrap_list();
        let f1 = f.child().unwrap_long();
        for j in 0..(batch_size * (batch_size - 1) / 2) {
            f1.write(j);
        }
        for j in 0..batch_size {
            f.write(j as u64);
        }
        
        let g = root.child(9).unwrap_map();
        let (gkey, gval) = g.children();
        let gkey_s = gkey.unwrap_string();
        let gval_b = gval.unwrap_boolean();
        for _ in 0..batch_size {
            gkey_s.write("param");
            gkey_s.write("setting");
        }
        for j in 0..batch_size {
            gval_b.write((j % 2) != 0);
            gval_b.write(((j + 1) % 2) != 0);
        }
        for _ in 0..batch_size {
            g.write(2);
        }

        let h = root.child(10).unwrap_timestamp();
        for j in 0..batch_size {
            h.write_nanos(j, 10u32.pow((j % 9) as u32));
        }

        let i = root.child(11).unwrap_union();
        for j in 0..batch_size {
            if j % 2 == 0 {
                i.child(0).unwrap_long().write(j);
                i.write(0);
            } else {
                i.child(1).unwrap_float().write(j as f32);
                i.write(1);
            }
        }

        let k = root.child(12).unwrap_binary();
        for _ in 0..batch_size {
            k.write(b"abc");
        }

        for _ in 0..batch_size {
            root.write();
        }
        writer.write_batch(batch_size as u64)?;
        
        let data = writer.data();
        let root = data.unwrap_struct();
        root.write_null();
        writer.write_batch(1)?;
    }
    writer.finish()?;
    Ok(())
}
