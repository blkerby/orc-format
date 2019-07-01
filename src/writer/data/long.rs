use std::io::{Write, Result};
use std::mem;

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, SignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, LongStatistics};
use crate::writer::data::common::{GenericData, BaseData, write_index};


pub struct LongData {
    pub(crate) column_id: u32,
    present: BooleanRLE,
    data: SignedIntRLEv1,
    stripe_stats: LongStatistics,
    row_group_stats: LongStatistics,
    row_group_position: Vec<u64>,
    row_index_entries: Vec<orc_proto::RowIndexEntry>,
    schema: Schema,
    config: Config,
}

impl LongData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        let mut out = Self {
            column_id: cid,
            present: BooleanRLE::new(&config.compression),
            data: SignedIntRLEv1::new(&config.compression),
            stripe_stats: LongStatistics::new(),
            row_group_stats: LongStatistics::new(),
            row_group_position: Vec::new(),
            row_index_entries: Vec::new(),
            schema: schema.clone(),
            config: config.clone(),
        };
        out.record_position();
        out
    }

    fn record_position(&mut self) {
        self.row_group_position.clear();
        self.present.record_position(&mut self.row_group_position);
        self.data.record_position(&mut self.row_group_position);
    }

    fn finish_row_group(&mut self) {
        if self.row_group_stats.num_values > 0 {
            self.stripe_stats.merge(&self.row_group_stats);
            
            let mut row_index_entry = orc_proto::RowIndexEntry::new();
            row_index_entry.set_positions(self.row_group_position.clone());
            row_index_entry.set_statistics(Statistics::Long(self.row_group_stats).to_proto());
            self.row_index_entries.push(row_index_entry);
            
            self.record_position();
            self.row_group_stats = LongStatistics::new();
        }
    }

    fn check_row_group(&mut self) {
        if self.row_group_stats.num_values == self.config.row_index_stride as u64 {
            self.finish_row_group();
        }
    }

    pub fn write(&mut self, x: i64) {
        self.present.write(true);
        self.data.write(x);
        self.row_group_stats.update(x);
        self.check_row_group();
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl GenericData for LongData {
    fn write_null(&mut self) {
        self.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}


impl BaseData for LongData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        self.finish_row_group();
        let row_index_entries = mem::replace(&mut self.row_index_entries, Vec::new());
        println!("row_index {:?}", &row_index_entries);
        write_index(row_index_entries, self.column_id(), &self.config.compression, out, stream_infos_out)?;
        Ok(())
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        if self.stripe_stats.has_null() {
            let present_start_pos = out.pos();
            self.present.finish(out)?;
            let present_len = (out.pos() - present_start_pos) as u64;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len,
            });
        }

        let data_start_pos = out.pos();
        self.data.finish(out)?;
        let data_len = (out.pos() - data_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: data_len,
        });

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Long(self.stripe_stats));
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.data.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Long), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }
}
