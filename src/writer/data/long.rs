use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::encoder::{BooleanRLE, SignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, LongStatistics};
use crate::writer::data::common::BaseData;



pub struct LongData<'a> {
    pub(crate) column_id: u32,
    pub(crate) config: &'a Config,
    pub(crate) schema: &'a Schema,
    present: BooleanRLE,
    data: SignedIntRLEv1,
    current_row_group_stats: LongStatistics,
    row_group_stats: Vec<LongStatistics>,
    splice_stats: LongStatistics,
}

impl<'a> LongData<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        LongData {
            column_id: cid,
            config,
            schema,
            present: BooleanRLE::new(&config.compression),
            data: SignedIntRLEv1::new(&config.compression),
            current_row_group_stats: LongStatistics::new(),
            row_group_stats: vec![],
            splice_stats: LongStatistics::new(),
        }
    }

    pub fn write(&mut self, x: Option<i64>) {
        match x {
            Some(y) => {
                self.present.write(true);
                self.data.write(y);
            }
            None => { 
                self.present.write(false); 
            }
        }
        self.current_row_group_stats.update(x);
        if self.current_row_group_stats.num_rows >= self.config.row_index_stride as u64 {
            self.splice_stats.merge(&self.current_row_group_stats);
            self.row_group_stats.push(self.current_row_group_stats);
            self.current_row_group_stats = LongStatistics::new();
        }
    }
}

impl<'a> BaseData<'a> for LongData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
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
        Ok(total_len)
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Long(self.splice_stats));
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.data.estimated_size()
    }

    fn verify_row_count(&self, row_count: u64, batch_size: u64) {
        let rows_written = self.splice_stats.num_rows + self.current_row_group_stats.num_rows;
        if rows_written != row_count {
            let prior_num_rows = row_count - batch_size;
            panic!("In Long column {}, the number of values written ({}) does not match the batch size ({})", 
                self.column_id, rows_written - prior_num_rows, batch_size);
        }
    }

    fn reset(&mut self) {
        self.splice_stats = LongStatistics::new();
        self.current_row_group_stats = LongStatistics::new();
        self.row_group_stats = vec![];
    }
}
