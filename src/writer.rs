use super::protos::orc_proto;
use super::schema::Schema;
use protobuf::{CodedOutputStream, Message, RepeatedField};
use std::io::{Result, Write};
use std::slice;

use count_write::CountWrite;
use statistics::{BaseStatistics, Statistics};
use stripe::{Stripe, StripeInfo};

use data::{Data, BaseData};
use compression::{Compression, CompressionStream, NoCompression};

pub mod compression;
pub mod data;
mod stripe;
mod encoder;
mod statistics;
mod count_write;


#[derive(Clone)]
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
pub struct Writer<W: Write> {
    inner: CountWrite<W>,
    config: Config,
    current_stripe: Stripe,
    stripe_infos: Vec<StripeInfo>,
}

impl<W: Write> Writer<W> {
    const HEADER_LENGTH: u64 = 3;

    pub fn new(inner: W, schema: &Schema, config: Config) -> Result<Self> {
        let mut writer = Self {
            inner: CountWrite::new(inner),
            current_stripe: Stripe::new(schema, &config),
            config,
            stripe_infos: Vec::new(),
        };
        writer.write_header()?;
        Ok(writer)
    }

    pub fn data(&mut self) -> &mut Data {
        &mut self.current_stripe.data
    }

    pub fn inner(&self) -> &W {
        &self.inner.inner
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.current_stripe.write_batch(num_rows)?;
        if self.current_stripe.data.estimated_size() > self.config.stripe_size {
            self.current_stripe.finish(&mut self.inner, &mut self.stripe_infos)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        self.current_stripe
            .finish(&mut self.inner, &mut self.stripe_infos)?;
        let content_length = self.current_stripe.offset - Self::HEADER_LENGTH;

        let metadata_start_pos = self.inner.pos();
        self.write_metadata()?;

        let footer_start_pos = self.inner.pos();
        self.write_footer(content_length)?;

        let postscript_start_pos = self.inner.pos();
        let metadata_length = (footer_start_pos - metadata_start_pos) as u64;
        let footer_length = (postscript_start_pos - footer_start_pos) as u64;
        self.write_postscript(metadata_length, footer_length)?;

        let end_pos = self.inner.pos();
        let postscript_length = (end_pos - postscript_start_pos) as u8;
        self.inner.write(slice::from_ref(&postscript_length))?;

        self.inner.flush()?;
        Ok(self.inner.into_inner())
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

    fn write_metadata(&mut self) -> Result<()> {
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
        compression_stream.finish(&mut self.inner)?;
        Ok(())
    }

    fn make_types(data: &Data, types: &mut Vec<orc_proto::Type>) {
        let mut t = orc_proto::Type::new();
        match data {
            Data::Boolean(_) => {
                t.set_kind(orc_proto::Type_Kind::BOOLEAN);
                types.push(t);
            }
            Data::Long(long_data) => {
                t.set_kind(match long_data.schema() {
                    Schema::Short => orc_proto::Type_Kind::SHORT,
                    Schema::Int => orc_proto::Type_Kind::INT,
                    Schema::Long => orc_proto::Type_Kind::LONG,
                    Schema::Date => orc_proto::Type_Kind::DATE,
                    _ => unreachable!(),
                });
                types.push(t);
            }
            Data::Float(_) => {
                t.set_kind(orc_proto::Type_Kind::FLOAT);
                types.push(t);
            }
            Data::Double(_) => {
                t.set_kind(orc_proto::Type_Kind::DOUBLE);
                types.push(t);
            }
            Data::Timestamp(_) => {
                t.set_kind(orc_proto::Type_Kind::TIMESTAMP);
                types.push(t);
            }
            Data::Decimal(d) => {
                t.set_kind(orc_proto::Type_Kind::DECIMAL);
                t.set_precision(d.precision());
                t.set_scale(d.scale());
                types.push(t);
            }
            Data::String(d) => {
                match d.schema() {
                    Schema::String => {
                        t.set_kind(orc_proto::Type_Kind::STRING);
                    }
                    Schema::Char(n) => {
                        t.set_kind(orc_proto::Type_Kind::CHAR);
                        t.set_maximumLength(*n);
                    }
                    Schema::VarChar(n) => {
                        t.set_kind(orc_proto::Type_Kind::VARCHAR);
                        t.set_maximumLength(*n);
                    }
                    _ => unreachable!()
                }
                types.push(t);
            }
            Data::Binary(_) => {
                t.set_kind(orc_proto::Type_Kind::BINARY);
                types.push(t);
            }
            Data::Struct(struct_data) => {
                t.set_kind(orc_proto::Type_Kind::STRUCT);
                let mut subtypes: Vec<u32> = Vec::new();
                for d in &struct_data.children {
                    subtypes.push(d.column_id());
                }

                let mut field_names: Vec<String> = Vec::new();
                for f in struct_data.field_names() {
                    field_names.push(f.clone());
                }

                t.set_subtypes(subtypes);
                t.set_fieldNames(RepeatedField::from_vec(field_names));
                types.push(t);
                for d in &struct_data.children {
                    Self::make_types(d, types);
                }
            }
            Data::Union(d) => {
                t.set_kind(orc_proto::Type_Kind::UNION);
                let mut subtypes: Vec<u32> = Vec::new();
                for e in &d.children {
                    subtypes.push(e.column_id());
                }
                t.set_subtypes(subtypes);
                types.push(t);
                for e in &d.children {
                    Self::make_types(e, types);
                }
            }
            Data::List(d) => {
                t.set_kind(orc_proto::Type_Kind::LIST);
                t.set_subtypes(vec![d.child.column_id()]);
                types.push(t);
                Self::make_types(&d.child, types);
            }
            Data::Map(d) => {
                t.set_kind(orc_proto::Type_Kind::MAP);
                t.set_subtypes(vec![d.keys.column_id(), d.values.column_id()]);
                types.push(t);
                Self::make_types(&d.keys, types);
                Self::make_types(&d.values, types);
            }
        }
    }

    fn write_footer(&mut self, content_length: u64) -> Result<()> {
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
        compression_stream.finish(&mut self.inner)?;
        Ok(())
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
        Ok(())
    }
}
