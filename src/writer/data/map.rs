use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, UnsignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, GenericStatistics};
use crate::writer::data::common::BaseData;
use crate::writer::data::{GenericData, Data};

pub struct MapData {
    column_id: u32,
    pub(crate) keys: Box<Data>,
    pub(crate) values: Box<Data>,
    present: BooleanRLE,
    lengths: UnsignedIntRLEv1,
    stripe_stats: GenericStatistics,
    num_child_values: u64,
}

impl MapData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        if let Schema::Map(key_schema, value_schema) = schema {                
            Self {
                column_id: cid,
                keys: Box::new(Data::new(&key_schema, config, column_id)),
                values: Box::new(Data::new(&value_schema, config, column_id)),
                present: BooleanRLE::new(&config.compression),
                lengths: UnsignedIntRLEv1::new(&config.compression),
                stripe_stats: GenericStatistics::new(),
                num_child_values: 0,
            }
        } else { unreachable!() }
    }
    
    pub fn keys(&mut self) -> &mut Data {
        &mut self.keys
    }

    pub fn values(&mut self) -> &mut Data {
        &mut self.values
    }

    pub fn children(&mut self) -> (&mut Data, &mut Data) {
        (&mut self.keys, &mut self.values)
    }

    pub fn write(&mut self, len: u64) {
        self.present.write(true);
        self.lengths.write(len);
        self.num_child_values += len;
        self.stripe_stats.update();
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl GenericData for MapData {
    fn write_null(&mut self) {
        self.present.write(false);
        self.stripe_stats.update_null();
    }
}

impl BaseData for MapData {
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

        let lengths_start_pos = out.pos();
        self.lengths.finish(out)?;
        let lengths_len = (out.pos() - lengths_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::LENGTH,
            column_id: self.column_id,
            length: lengths_len,
        });

        self.keys.write_data_streams(out, stream_infos_out)?;
        self.values.write_data_streams(out, stream_infos_out)?;

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        self.keys.column_encodings(out);
        self.values.column_encodings(out);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Generic(self.stripe_stats));
        self.keys.statistics(out);
        self.values.statistics(out);
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Map), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
        self.keys.verify_row_count(self.num_child_values);
        self.values.verify_row_count(self.num_child_values);
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.lengths.estimated_size() + 
            self.keys.estimated_size() + self.values.estimated_size()
    }
}
