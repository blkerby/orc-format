mod protos;

use std::io::Write;

use protos::orc_proto;
use protobuf::{RepeatedField, CodedOutputStream, Message};
use protobuf::error::ProtobufResult;

struct Field {
    name: String,
    schema: TypeDescription
}

enum TypeDescription {
    Int,
    Struct(Vec<Field>)
}

struct Writer<'a, T: Write> {
    inner: T,
    schema: &'a TypeDescription
}

impl<'a, T: Write> Writer<'a, T> {
    pub fn new(inner: T, schema: &'a TypeDescription) -> Writer<T> {
        Writer { 
            inner,
            schema
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer() {
        let schema = TypeDescription::Int;
        let mut v: Vec<u8> = vec![];
        let mut writer = Writer::new(&mut v, &schema);
        writer.write_footer();
        println!("{:?}", v);
    }
}
