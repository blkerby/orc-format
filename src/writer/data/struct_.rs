use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Field;
use crate::writer::Config;
use crate::writer::encoder::BooleanRLE;
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, StructStatistics};
use crate::writer::data::common::BaseData;
use crate::writer::data::Data;

pub struct StructData<'a> {
    column_id: u32,
    pub(crate) config: &'a Config,
    pub(crate) fields: &'a [Field],
    pub(crate) children: Vec<Data<'a>>,
    present: BooleanRLE,
    splice_stats: StructStatistics,
    num_nulls: u64,
}

impl<'a> StructData<'a> {
    pub(crate) fn new(fields: &'a [Field], config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        let mut children: Vec<Data> = Vec::new();
        *column_id += 1;
        for field in fields {
            children.push(Data::new(&field.1, config, column_id));
        }

        StructData {
            column_id: cid,
            fields,
            config,
            present: BooleanRLE::new(&config.compression),
            children: children,
            splice_stats: StructStatistics::new(),
            num_nulls: 0,
        }
    }

    pub fn children(&mut self) -> &mut [Data<'a>] {
        &mut self.children
    }

    pub fn write(&mut self, present: bool) {
        self.num_nulls += (!present) as u64;
        self.present.write(present);
        self.splice_stats.update(present);
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl<'a> BaseData<'a> for StructData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut W, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        Ok(0)
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        let present_len = self.present.finish(out)?;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::PRESENT,
            column_id: self.column_id,
            length: present_len as u64,
        });
        let mut children_len = 0;
        for child in &mut self.children {
            children_len += child.write_data_streams(out, stream_infos_out)?;
        }
        Ok(present_len + children_len)
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        for child in &self.children {
            child.column_encodings(out);
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Struct(self.splice_stats));
        for child in &self.children {
            child.statistics(out);
        }
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.splice_stats.num_rows;
        if rows_written != expected_row_count {
            panic!("In Struct column {}, the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }

        for child in &self.children {
            child.verify_row_count(expected_row_count - self.num_nulls);
        }
    }

    fn estimated_size(&self) -> usize {
        let mut size = 0;
        for child in &self.children {
            size += child.estimated_size();
        }
        size
    }

    fn reset(&mut self) {
        self.splice_stats = StructStatistics::new();
        for child in &mut self.children {
            child.reset();
        }
    }
}
