use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, UnsignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, GenericStatistics};
use crate::writer::data::common::BaseData;
use crate::writer::data::Data;

pub struct ListData<'a> {
    column_id: u32,
    pub(crate) schema: &'a Schema,
    pub(crate) child: Box<Data<'a>>,
    present: BooleanRLE,
    lengths: UnsignedIntRLEv1,
    stripe_stats: GenericStatistics,
    num_child_values: u64,
}

impl<'a> ListData<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        if let Schema::List(child_schema) = schema {                
            Self {
                column_id: cid,
                schema,
                child: Box::new(Data::new(&child_schema, config, column_id)),
                present: BooleanRLE::new(&config.compression),
                lengths: UnsignedIntRLEv1::new(&config.compression),
                stripe_stats: GenericStatistics::new(),
                num_child_values: 0,
            }
        } else { unreachable!() }
    }
    
    pub fn child(&mut self) -> &mut Data<'a> {
        &mut self.child
    }

    pub fn write(&mut self, len: Option<u64>) {
        if let Some(l) = len {
            self.present.write(true);
            self.lengths.write(l);
            self.num_child_values += l;
        } else {
            self.present.write(false);
        }
        self.stripe_stats.update(len.is_some());
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl<'a> BaseData<'a> for ListData<'a> {
    fn schema(&self) -> &'a Schema { self.schema }

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

        self.child.write_data_streams(out, stream_infos_out)?;

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        self.child.column_encodings(out);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Generic(self.stripe_stats));
        self.child.statistics(out);
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type List), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
        self.child.verify_row_count(self.num_child_values);
    }

    fn estimated_size(&self) -> usize {
        self.present.estimated_size() + self.lengths.estimated_size() + 
            self.child.estimated_size()
    }

    fn reset(&mut self) {
        self.stripe_stats = GenericStatistics::new();
        self.child.reset();
    }
}
