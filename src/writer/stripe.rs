use std::io::{Write, Result};
use protobuf::{CodedOutputStream, RepeatedField, Message};

use crate::protos::orc_proto;
use crate::schema::Schema;

use super::count_write::CountWrite;
use super::Config;
use super::data::{Data, BaseData};
use super::statistics::Statistics;
use super::compression::CompressionStream;

#[derive(Debug)]
pub struct StripeInfo {
    pub offset: u64,
    pub num_rows: u64,
    pub index_length: u64,
    pub data_length: u64,
    pub footer_length: u64,
    pub statistics: Vec<Statistics>,
}

#[derive(Debug)]
pub struct StreamInfo {
    pub kind: orc_proto::Stream_Kind,
    pub column_id: u32,
    pub length: u64,
}

pub struct Stripe {
    pub data: Data,
    pub offset: u64,
    pub num_rows: u64,
    pub schema: Schema,
    pub config: Config,
}

impl Stripe {
    pub fn new(schema: &Schema, config: &Config) -> Self {
        Stripe {
            data: Data::new(schema, config, &mut 0),
            offset: 3,
            num_rows: 0,
            schema: schema.clone(),
            config: config.clone(),
        }
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.num_rows += num_rows;
        self.data.verify_row_count(self.num_rows);
        Ok(())
    }

    fn write_footer<W: Write>(&mut self, out: &mut W, stream_infos: &[StreamInfo]) -> Result<()> {
        let mut compressed_stream = CompressionStream::new(&self.config.compression);
        let mut coded_out = CodedOutputStream::new(&mut compressed_stream);
        let mut footer = orc_proto::StripeFooter::new();
        
        let mut streams: Vec<orc_proto::Stream> = Vec::new();
        for si in stream_infos {
            let mut stream = orc_proto::Stream::new();
            stream.set_kind(si.kind);
            stream.set_column(si.column_id);
            stream.set_length(si.length);
            streams.push(stream);
        }
        footer.set_streams(RepeatedField::from_vec(streams));
        
        let mut encodings: Vec<orc_proto::ColumnEncoding> = Vec::new();
        self.data.column_encodings(&mut encodings);
        footer.set_columns(RepeatedField::from_vec(encodings));

        footer.write_to(&mut coded_out)?;
        coded_out.flush()?;
        compressed_stream.finish(out)?;
        Ok(())
    }

    pub fn finish<W: Write>(&mut self, out: &mut CountWrite<W>, stripe_infos_out: &mut Vec<StripeInfo>) -> Result<()> {
        if self.num_rows == 0 { return Ok(()) }
        let mut stream_infos: Vec<StreamInfo> = Vec::new();
        let mut statistics: Vec<Statistics> = Vec::new();
        self.data.statistics(&mut statistics);

        let index_start_pos = out.pos();
        self.data.write_index_streams(out, &mut stream_infos)?;

        let data_start_pos = out.pos();
        self.data.write_data_streams(out, &mut stream_infos)?;
        
        let footer_start_pos = out.pos();
        self.write_footer(out, &stream_infos)?;

        let end_pos = out.pos();
        let index_length = (data_start_pos - index_start_pos) as u64;
        let data_length = (footer_start_pos - data_start_pos) as u64;
        let footer_length = (end_pos - footer_start_pos) as u64;
        stripe_infos_out.push(StripeInfo {
            offset: self.offset,
            num_rows: self.num_rows,
            index_length,
            data_length,
            footer_length,
            statistics,
        });
        self.offset += index_length + data_length + footer_length;
        self.num_rows = 0;
        self.data = Data::new(&self.schema, &self.config, &mut 0);
        Ok(())
    }
}