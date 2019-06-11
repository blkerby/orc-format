use std::io::{Write, Result};
use protobuf::{RepeatedField, CodedOutputStream, Message};
use protobuf::error::ProtobufResult;

use super::protos::orc_proto;
use super::schema::Schema;
use data::Data;
use statistics::Statistics;

mod encoder;
mod data;
mod statistics;


struct Buffer(Vec<u8>);

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Buffer {
    pub fn new() -> Self {
        Buffer(Vec::new())
    }

    pub fn drain<W: Write>(&mut self, out: &mut W) {
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

struct StripeInfo {
    offset: u64,
    index_length: u64,
    data_length: u64,
    footer_length: u64,
    number_of_rows: u64,
    statistics: Statistics,
}

struct Stripe {
    data: Data,
    num_rows: u64,
}

pub struct Config {
    row_index_stride: u32,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            row_index_stride: 10000,
        }
    }
}

#[must_use]
pub struct Writer<'a, W: Write> {
    inner: W,
    schema: &'a Schema,
    config: &'a Config,
    stripe_infos: Vec<StripeInfo>,
    stripe: Stripe,
    offset: u64,

}

impl<'a, W: Write> Writer<'a, W> {
    const HEADER_LENGTH: u64 = 3;

    pub fn new(inner: W, schema: &'a Schema, config: &'a Config) -> Writer<'a, W> {
        Writer { 
            inner,
            schema,
            config,
            stripe_infos: Vec::new(),
            stripe: Stripe {
                data: Data::new(schema, &mut 0),
                num_rows: 0,
            },
            offset: 0,
        }
    }

    pub fn data(&mut self) -> &mut Data {
        &mut self.stripe.data
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.stripe.num_rows += num_rows;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()>{
        self.write_stripe()?;
        let content_length = self.offset - Self::HEADER_LENGTH;
        let metadata_length = self.write_metadata()?;
        let footer_length = self.write_footer(content_length)?;
        self.write_postscript(metadata_length, footer_length)?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.inner.write(b"ORC")?;
        self.offset = Self::HEADER_LENGTH;
        Ok(())
    }

    fn write_stripe(&mut self) -> Result<()> {
        if self.stripe_infos.is_empty() {
            self.write_header();
        }
        if self.stripe.num_rows > 0 {
        }
        Ok(())
    }

    fn write_metadata(&mut self) -> Result<u64> {
        Ok(0)
    }

    fn make_types(schema: &Schema, types: &mut Vec<orc_proto::Type>) {
        let mut t = orc_proto::Type::new();
        match schema {
            Schema::Long => {
                t.set_kind(orc_proto::Type_Kind::LONG);
                types.push(t);
            }
            Schema::Struct(fields) => {
                t.set_kind(orc_proto::Type_Kind::STRUCT);
                // t.subtypes
            }
        }
    }

    fn write_footer(&mut self, content_length: u64) -> Result<u64> {
        let mut coded_out = CodedOutputStream::new(&mut self.inner);
        let mut footer = orc_proto::Footer::new();
        footer.set_headerLength(Self::HEADER_LENGTH);
        footer.set_contentLength(content_length);
        
        let mut stripes: Vec<orc_proto::StripeInformation> = Vec::new();
        for stripe_info in &self.stripe_infos {
            let mut stripe = orc_proto::StripeInformation::new();
            stripe.set_offset(stripe_info.offset);
            stripe.set_indexLength(stripe_info.index_length);
            stripe.set_dataLength(stripe_info.data_length);
            stripe.set_footerLength(stripe_info.data_length);
            stripe.set_numberOfRows(stripe_info.number_of_rows);
        }
        footer.set_stripes(RepeatedField::from_vec(stripes));
        
        let mut types: Vec<orc_proto::Type> = Vec::new();
        Self::make_types(self.schema, &mut types);
        footer.set_types(RepeatedField::from_vec(types));

        footer.set_metadata(RepeatedField::from_vec(vec![]));
        footer.set_numberOfRows(self.stripe_infos.iter().map(|x| x.number_of_rows).sum());
        footer.set_statistics(RepeatedField::from_vec(vec![]));
        footer.set_rowIndexStride(self.config.row_index_stride);
        
        footer.write_to(&mut coded_out)?;
        coded_out.flush()?;
        Ok(footer.compute_size() as u64)
    }

    fn write_postscript(&mut self, metadata_length: u64, footer_length: u64) -> Result<()> {
        let mut coded_out = CodedOutputStream::new(&mut self.inner);
        let mut postscript = orc_proto::PostScript::new();
        postscript.set_compression(orc_proto::CompressionKind::NONE);
        postscript.set_compressionBlockSize(0);
        postscript.set_writerVersion(1);
        postscript.set_metadataLength(metadata_length);
        postscript.set_footerLength(footer_length);
        postscript.set_version(vec![0, 12]);
        postscript.set_magic("ORC".to_owned());
        coded_out.flush()?;
        Ok(())
    }
}
