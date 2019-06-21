use super::protos::orc_proto;
use super::schema::Schema;
use protobuf::{CodedOutputStream, Message, RepeatedField};
use std::io::{Result, Write};
use std::slice;

use statistics::{BaseStatistics, Statistics};
use stripe::{Stripe, StripeInfo};

pub use data::{Data, BaseData, LongData, StructData};
pub use compression::snappy::SnappyCompression;
pub use compression::{Compression, CompressionStream, NoCompression};

mod compression;
mod stripe;
mod data;
mod encoder;
mod statistics;


pub struct Config {
    row_index_stride: u32,
    compression: Compression,
    stripe_size: usize,
}

impl Config {
    pub fn new() -> Config {
        Config {
            row_index_stride: 10000,
            compression: NoCompression::new().build(),
            stripe_size: 67108864,
        }
    }

    pub fn with_row_index_stride(mut self, row_index_stride: u32) -> Self {
        self.row_index_stride = row_index_stride;
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_stripe_size(mut self, stripe_size: usize) -> Self {
        self.stripe_size = stripe_size;
        self
    }
}

#[must_use]
pub struct Writer<'a, W: Write> {
    inner: W,
    config: &'a Config,
    current_stripe: Stripe<'a>,
    stripe_infos: Vec<StripeInfo>,
}

impl<'a, W: Write> Writer<'a, W> {
    const HEADER_LENGTH: u64 = 3;

    pub fn new(inner: W, schema: &'a Schema, config: &'a Config) -> Result<Writer<'a, W>> {
        let mut writer = Writer {
            inner,
            config,
            current_stripe: Stripe::new(&schema, &config),
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
        if self.current_stripe.data.estimated_size() > self.config.stripe_size {
            self.current_stripe.finish(&mut self.inner, &mut self.stripe_infos)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.current_stripe
            .finish(&mut self.inner, &mut self.stripe_infos)?;
        let content_length = self.current_stripe.offset - Self::HEADER_LENGTH;
        let metadata_length = self.write_metadata()?;
        let footer_length = self.write_footer(content_length)?;
        self.write_postscript(metadata_length, footer_length)?;
        self.inner.flush()?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.inner.write(b"ORC")?;
        Ok(())
    }

    fn merge_statistics(&self) -> Vec<Statistics> {
        let mut statistics: Vec<Statistics> = Vec::new();
        self.current_stripe.data.statistics(&mut statistics);
        for si in &self.stripe_infos {
            for (i, stat) in si.statistics.iter().enumerate() {
                statistics[i].merge(stat);
            }
        }
        statistics
    }

    fn write_metadata(&mut self) -> Result<u64> {
        let mut compression_stream = CompressionStream::new(&self.config.compression);
        let mut coded_out = CodedOutputStream::new(&mut compression_stream);
        let mut metadata = orc_proto::Metadata::new();
        let mut stripe_statistics: Vec<orc_proto::StripeStatistics> = Vec::new();
        for stripe_info in &self.stripe_infos {
            let mut stripe_stats = orc_proto::StripeStatistics::new();
            let mut col_stats: Vec<orc_proto::ColumnStatistics> = Vec::new();
            for info_stat in &stripe_info.statistics {
                col_stats.push(info_stat.to_proto());
            }
            stripe_stats.set_colStats(RepeatedField::from_vec(col_stats));
            stripe_statistics.push(stripe_stats);
        }
        metadata.set_stripeStats(RepeatedField::from_vec(stripe_statistics));
        metadata.write_to(&mut coded_out)?;
        coded_out.flush()?;
        let size = compression_stream.finish(&mut self.inner)?;
        Ok(size as u64)
    }

    fn make_types(data: &Data<'a>, types: &mut Vec<orc_proto::Type>) {
        let mut t = orc_proto::Type::new();
        match data {
            Data::Long(long_data) => {
                t.set_kind(match long_data.schema {
                    Schema::Short => orc_proto::Type_Kind::SHORT,
                    Schema::Int => orc_proto::Type_Kind::INT,
                    Schema::Long => orc_proto::Type_Kind::LONG,
                    _ => unreachable!(),
                });
                types.push(t);
            }
            Data::String(string_data) => {
                t.set_kind(orc_proto::Type_Kind::STRING);
                types.push(t);
            }
            Data::Struct(struct_data) => {
                t.set_kind(orc_proto::Type_Kind::STRUCT);
                let mut subtypes: Vec<u32> = Vec::new();
                for d in &struct_data.children {
                    subtypes.push(d.column_id());
                }

                let mut field_names: Vec<String> = Vec::new();
                for f in struct_data.fields {
                    field_names.push(f.0.to_owned());
                }

                t.set_subtypes(subtypes);
                t.set_fieldNames(RepeatedField::from_vec(field_names));
                types.push(t);
                for d in &struct_data.children {
                    Self::make_types(d, types);
                }
            }
        }
    }

    fn write_footer(&mut self, content_length: u64) -> Result<u64> {
        let mut compression_stream = CompressionStream::new(&self.config.compression);
        let stats: Vec<_> = self
            .merge_statistics()
            .iter()
            .map(|x| x.to_proto())
            .collect();
        let mut coded_out = CodedOutputStream::new(&mut compression_stream);
        let mut footer = orc_proto::Footer::new();
        footer.set_headerLength(Self::HEADER_LENGTH);
        footer.set_contentLength(content_length);

        let mut stripes: Vec<orc_proto::StripeInformation> = Vec::new();
        for stripe_info in &self.stripe_infos {
            let mut stripe = orc_proto::StripeInformation::new();
            stripe.set_offset(stripe_info.offset);
            stripe.set_indexLength(stripe_info.index_length);
            stripe.set_dataLength(stripe_info.data_length);
            stripe.set_footerLength(stripe_info.footer_length);
            stripe.set_numberOfRows(stripe_info.num_rows);
            stripes.push(stripe);
        }
        footer.set_stripes(RepeatedField::from_vec(stripes));

        let mut types: Vec<orc_proto::Type> = Vec::new();
        Self::make_types(&self.current_stripe.data, &mut types);
        footer.set_types(RepeatedField::from_vec(types));

        footer.set_metadata(RepeatedField::from_vec(vec![]));
        footer.set_numberOfRows(self.stripe_infos.iter().map(|x| x.num_rows).sum());
        footer.set_statistics(RepeatedField::from_vec(stats));
        footer.set_rowIndexStride(self.config.row_index_stride);

        footer.write_to(&mut coded_out)?;
        coded_out.flush()?;
        let size = compression_stream.finish(&mut self.inner)?;
        Ok(size as u64)
    }

    fn write_postscript(&mut self, metadata_length: u64, footer_length: u64) -> Result<()> {
        let mut coded_out = CodedOutputStream::new(&mut self.inner);
        let mut postscript = orc_proto::PostScript::new();
        postscript.set_compression(self.config.compression.kind());
        postscript.set_compressionBlockSize(self.config.compression.block_size() as u64);
        postscript.set_writerVersion(6);
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
