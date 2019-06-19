
mod buffer;
mod protos;
pub mod reader;
pub mod schema;
pub mod writer;

// #[cfg(test)]
// mod tests {
//     use super::schema::*;
//     use crate::writer::{Config, Writer, Data};
//     use std::fs::File;
//     use std::io::Result;
    
//     #[test]
//     fn writer() -> Result<()> {
//         let schema = Schema::Long;
//         let mut out = File::create("target/test.orc")?;
//         let config = Config::default();
//         let mut writer = Writer::new(&mut out, &schema, &config)?;
//         for n in 0..10 {
//             let data = writer.data();
//             if let Data::Long(data) = data {
//                 for i in 0..1000000 {
//                     data.write(Some(i % 2));
//                 }
//             } else {
//                 unreachable!();
//             }
//             writer.write_batch(1000000)?;
//         }
//         writer.finish()?;
//         Ok(())
//     }
// }
