use std::io::Write;
use protobuf::{RepeatedField, CodedOutputStream, Message};
use protobuf::error::ProtobufResult;

use super::protos::orc_proto;
use super::schema::Schema;
use data::Data;

mod encoder;
mod data;
mod batch;


struct Buffer(Vec<u8>);

impl Buffer {
    pub fn new() -> Self {
        Buffer(Vec::new())
    }

    pub fn write_u8(&mut self, x: u8) {
        self.0.push(x);
    }

    pub fn flush<W: Write>(&mut self, out: &mut W) {
        out.write_all(&self.0);
        self.0.clear()
    }
}

// struct TypeWriter<'a, T: Write> {
//     writer: &'a mut Writer<'a, T>,
//     next_id: u32
// }

// impl<'a, T: Write> TypeWriter<'a, T> {
//     pub fn new(writer: &'a mut Writer<'a, T>) -> Self {
//         TypeWriter { writer, next_id: 0 }
//     }

//     fn write_type(&mut self, td: &TypeDescription, id: u32) {
//         let mut out = CodedOutputStream::new(&mut self.writer.inner);
//         let mut ty = orc_proto::Type::new();
//         ty.set_kind(match td {
//             TypeDescription::Int => orc_proto::Type_Kind::INT,
//             TypeDescription::Struct(_) => orc_proto::Type_Kind::STRUCT
//         });
        
//     }

//     pub fn write_types(&mut self) {
//         self.write_type(self.writer.schema, 0);
//     }
// }

#[must_use]
pub struct Writer<'a, W: Write> {
    inner: W,
    schema: &'a Schema,
    data: Data,
}

impl<'a, W: Write> Writer<'a, W> {
    pub fn new(inner: W, schema: &'a Schema) -> Writer<W> {
        Writer { 
            inner,
            schema,
            data: Data::new(schema),
        }
    }

    pub fn batch_writer(self) -> BatchWriter<'a, W> {
        BatchWriter { writer: self }
    }

    pub fn finish(mut self) {
        self.write_footer();
    }

    fn write_types(&mut self) {
        
    }

    fn write_footer(&mut self) {
        let mut coded_out = CodedOutputStream::new(&mut self.inner);
        let mut footer = orc_proto::Footer::new();
        footer.set_headerLength(10);
        footer.set_contentLength(20);
        footer.set_stripes(RepeatedField::from_vec(vec![]));
        footer.write_to(&mut coded_out).unwrap();
        coded_out.flush().unwrap();
        //   repeated Type types = 4;
        //   repeated UserMetadataItem metadata = 5;
        //   optional uint64 numberOfRows = 6;
        //   repeated ColumnStatistics statistics = 7;
        //   optional uint32 rowIndexStride = 8;        
        
    }
}

#[must_use]
pub struct BatchWriter<'a, W: Write> {
    writer: Writer<'a, W>
}


impl<'a, W: Write> BatchWriter<'a, W> {
    pub fn finish(self) -> Writer<'a, W> {
        // TODO: actually write stuff here.
        self.writer
    }

    pub fn data(&mut self) -> &mut Data {
        &mut self.writer.data
    }
}
