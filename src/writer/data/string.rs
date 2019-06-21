use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::compression::CompressionStream;
use crate::writer::encoder::{BooleanRLE, UnsignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, StringStatistics};
use crate::writer::data::common::BaseData;

pub struct StringData<'a> {
    pub(crate) column_id: u32,
    schema: &'a Schema,
    present: BooleanRLE,
    data: CompressionStream,
    lengths: UnsignedIntRLEv1,
    splice_stats: StringStatistics,
}

impl<'a> StringData<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        StringData {
            column_id: cid,
            schema,
            present: BooleanRLE::new(&config.compression),
            data: CompressionStream::new(&config.compression),
            lengths: UnsignedIntRLEv1::new(&config.compression),
            splice_stats: StringStatistics::new(),
        }
    }

    pub fn write(&mut self, x: Option<&str>) {
        match x {
            Some(xv) => {
                self.present.write(true);
                self.data.write(xv.as_bytes());
                self.lengths.write(xv.len() as u64);
            }
            None => { 
                self.present.write(false); 
            }
        }
        self.splice_stats.update(x);
    }
}

impl<'a> BaseData<'a> for StringData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut W, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        Ok(0)
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        let mut total_len = 0;
        
        if self.splice_stats.has_null {
            let present_len = self.present.finish(out)?;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len as u64,
            });
            total_len += present_len;
        }
        
        let data_len = self.data.finish(out)?;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: data_len as u64,
        });
        total_len += data_len;

        let lengths_len = self.lengths.finish(out)?;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::LENGTH,
            column_id: self.column_id,
            length: lengths_len as u64,
        });
        total_len += lengths_len;
        Ok(total_len)
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::String(self.splice_stats.clone()));
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.data.estimated_size() + 
            self.lengths.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.splice_stats.num_rows;
        if rows_written != expected_row_count {
            panic!("In String column {}, the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }

    fn reset(&mut self) {
        self.splice_stats = StringStatistics::new();
    }
}
