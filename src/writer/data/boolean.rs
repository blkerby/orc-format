use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::BooleanRLE;
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, BooleanStatistics};
use crate::writer::data::common::BaseData;


pub struct BooleanData<'a> {
    pub(crate) column_id: u32,
    schema: &'a Schema,
    present: BooleanRLE,
    data: BooleanRLE,
    stripe_stats: BooleanStatistics,
}

impl<'a> BooleanData<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        BooleanData {
            column_id: cid,
            schema,
            present: BooleanRLE::new(&config.compression),
            data: BooleanRLE::new(&config.compression),
            stripe_stats: BooleanStatistics::new(),
        }
    }

    pub fn write(&mut self, x: Option<bool>) {
        match x {
            Some(xv) => {
                self.present.write(true);
                self.data.write(xv);
            }
            None => { 
                self.present.write(false); 
            }
        }
        self.stripe_stats.update(x);
    }
}

impl<'a> BaseData<'a> for BooleanData<'a> {
    fn schema(&self) -> &'a Schema { self.schema }

    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut CountWrite<W>, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        Ok(())
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        if self.stripe_stats.has_null() {
            let start_present_pos = out.pos();
            self.present.finish(out)?;
            let present_len = (out.pos() - start_present_pos) as u64;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len,
            });
        }
        
        let start_data_pos = out.pos();
        self.data.finish(out)?;
        let data_len = (out.pos() - start_data_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: data_len as u64,
        });
        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Boolean(self.stripe_stats.clone()));
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.data.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Boolean), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }

    fn reset(&mut self) {
        self.stripe_stats = BooleanStatistics::new();
    }
}
