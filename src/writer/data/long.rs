use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, SignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, LongStatistics};
use crate::writer::data::common::{GenericData, BaseData};


pub struct LongData {
    pub(crate) column_id: u32,
    present: BooleanRLE,
    data: SignedIntRLEv1,
    stripe_stats: LongStatistics,
    schema: Schema,
}

impl LongData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        Self {
            column_id: cid,
            present: BooleanRLE::new(&config.compression),
            data: SignedIntRLEv1::new(&config.compression),
            stripe_stats: LongStatistics::new(),
            schema: schema.clone(),
        }
    }

    pub fn write(&mut self, x: i64) {
        self.present.write(true);
        self.data.write(x);
        self.stripe_stats.update(x);
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl GenericData for LongData {
    fn write_null(&mut self) {
        self.present.write(false);
        self.stripe_stats.update_null();
    }
}

impl BaseData for LongData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut CountWrite<W>, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
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
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Long), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }

    fn reset(&mut self) {
        self.stripe_stats = LongStatistics::new();
    }
}
