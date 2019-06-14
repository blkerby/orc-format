use std::io::{Write, Result};
use protobuf::{CodedOutputStream, RepeatedField, Message};

use crate::protos::orc_proto;
use crate::schema::Schema;

use super::data::{Data, BaseData};
use super::statistics::Statistics;

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

pub struct Stripe<'a> {
    pub data: Data<'a>,
    pub offset: u64,
    pub num_rows: u64,
}

impl<'a> Stripe<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Stripe {
            data: Data::new(schema, &mut 0),
            offset: 3,
            num_rows: 0,
        }
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.num_rows += num_rows;
        Ok(())
    }

    fn write_footer<W: Write>(&mut self, out: &mut W, stream_infos: &[StreamInfo]) -> Result<u64> {
        let mut coded_out = CodedOutputStream::new(out);
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
        Ok(footer.compute_size() as u64)
    }

    pub fn finish<W: Write>(&mut self, out: &mut W, stripe_infos_out: &mut Vec<StripeInfo>) -> Result<()> {
        // if self.num_rows == 0 { return Ok(None) }
        let mut stream_infos: Vec<StreamInfo> = Vec::new();
        let mut statistics: Vec<Statistics> = Vec::new();
        self.data.statistics(&mut statistics);
        let index_length = self.data.write_index_streams(out, &mut stream_infos)?;
        let data_length = self.data.write_data_streams(out, &mut stream_infos)?;
        // self.data.reset();
        let footer_length = self.write_footer(out, &stream_infos)?;
        stripe_infos_out.push(StripeInfo {
            offset: self.offset,
            num_rows: self.num_rows,
            index_length,
            data_length,
            footer_length,
            statistics,
        });
        self.offset += index_length + data_length + footer_length;
        Ok(())
    }
}