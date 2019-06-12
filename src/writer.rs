use std::io::{Write, Result};
use std::slice;
use protobuf::{RepeatedField, CodedOutputStream, Message};
use protobuf::error::ProtobufResult;

use super::protos::orc_proto;
use super::schema::Schema;
use data::{Data, BaseData};
use stripe::{Stripe, StripeInfo};
use statistics::Statistics;

mod encoder;
mod data;
mod statistics;
mod stripe;


struct Buffer(Vec<u8>);

// impl Write for Buffer {
//     fn write(&mut self, buf: &[u8]) -> Result<usize> {
//         self.0.write(buf)
//     }

//     fn flush(&mut self) -> Result<()> {
//         Ok(())
//     }
// }

impl Buffer {
    pub fn new() -> Self {
        Buffer(Vec::new())
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> usize {
        let len = self.0.len();
        out.write_all(&self.0);
        self.0.clear();
        len
    }

    pub fn write_u8(&mut self, b: u8) {
        self.0.push(b);
    }

    pub fn write_bytes(&mut self, buf: &[u8]) {
        self.0.extend(buf);
    }
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
    current_stripe: Stripe<'a>,
    stripe_infos: Vec<StripeInfo>,
}

impl<'a, W: Write> Writer<'a, W> {
    const HEADER_LENGTH: u64 = 3;

    pub fn new(inner: W, schema: &'a Schema, config: &'a Config) -> Result<Writer<'a, W>> {
        let mut writer = Writer { 
            inner,
            schema,
            config,
            current_stripe: Stripe::new(schema),
            stripe_infos: Vec::new(),
        };
        writer.write_header()?;
        Ok(writer)
    }

    pub fn data(&mut self) -> &mut Data<'a> {
        &mut self.current_stripe.data
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.current_stripe.write_batch(num_rows)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()>{
        self.current_stripe.finish(&mut self.inner)?;
        let content_length = self.current_stripe.offset - Self::HEADER_LENGTH;
        let metadata_length = self.write_metadata()?;
        let footer_length = self.write_footer(content_length)?;
        self.write_postscript(metadata_length, footer_length)?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.inner.write(b"ORC")?;
        Ok(())
    }

    fn write_metadata(&mut self) -> Result<u64> {
        Ok(0)
    }

    fn make_types(data: &Data<'a>, types: &mut Vec<orc_proto::Type>) {
        let mut t = orc_proto::Type::new();
        match data {
            Data::Long(long_data) => {
                t.set_kind(match long_data.schema {
                    Schema::Short => orc_proto::Type_Kind::SHORT,
                    Schema::Int => orc_proto::Type_Kind::INT,
                    Schema::Long => orc_proto::Type_Kind::LONG,
                    _ => unreachable!()
                });
                types.push(t);
            }
            Data::Struct(struct_data) => {
                t.set_kind(orc_proto::Type_Kind::STRUCT);
                let mut subtypes: Vec<u32> = Vec::new();
                for d in &struct_data.children {
                    subtypes.push(d.column_id());
                }
                t.set_subtypes(subtypes);
                types.push(t);
                for d in &struct_data.children {
                    Self::make_types(d, types);
                }
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
            stripe.set_numberOfRows(stripe_info.num_rows);
        }
        footer.set_stripes(RepeatedField::from_vec(stripes));
        
        let mut types: Vec<orc_proto::Type> = Vec::new();
        Self::make_types(&self.current_stripe.data, &mut types);
        footer.set_types(RepeatedField::from_vec(types));

        footer.set_metadata(RepeatedField::from_vec(vec![]));
        footer.set_numberOfRows(self.stripe_infos.iter().map(|x| x.num_rows).sum());
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
        postscript.write_to(&mut coded_out)?;
        coded_out.flush()?;

        let size = postscript.compute_size() as u8;
        self.inner.write(slice::from_ref(&size))?;
        Ok(())
    }
}
