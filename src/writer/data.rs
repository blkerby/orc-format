use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::{Schema, Field};
use super::Config;
use super::encoder::{BooleanRLE, SignedIntRLEv1, UnsignedIntRLEv1};
use super::stripe::StreamInfo;
use super::statistics::{Statistics, BaseStatistics, LongStatistics, StructStatistics};
use super::compression::{Compression, CompressionStream};

pub trait BaseData<'a> {
    fn column_id(&self) -> u32;
    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64>;
    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64>;
    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>);
    fn statistics(&self, out: &mut Vec<Statistics>);
    fn verify_row_count(&self, num_rows: u64, batch_size: u64);
    fn estimated_size(&self) -> usize;
    fn reset(&mut self);
}

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

pub struct StructData<'a> {
    column_id: u32,
    pub(crate) config: &'a Config,
    pub(crate) fields: &'a [Field],
    pub(crate) children: Vec<Data<'a>>,
    present: BooleanRLE,
    current_row_group_stats: StructStatistics,
    row_group_stats: Vec<StructStatistics>,
    splice_stats: StructStatistics,
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
            current_row_group_stats: StructStatistics::new(),
            row_group_stats: vec![],
            splice_stats: StructStatistics::new(),
        }
    }

    pub fn children(&mut self) -> &mut [Data<'a>] {
        &mut self.children
    }

    pub fn write(&mut self, present: bool) {
        self.present.write(present);
        self.current_row_group_stats.update(present);
        if self.current_row_group_stats.num_rows >= self.config.row_index_stride as u64 {
            self.splice_stats.merge(&self.current_row_group_stats);
            self.row_group_stats.push(self.current_row_group_stats);
            self.current_row_group_stats = StructStatistics::new();
        }
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl<'a> BaseData<'a> for StructData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
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
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Struct(self.splice_stats));
        for child in &self.children {
            child.statistics(out);
        }
    }

    fn verify_row_count(&self, row_count: u64, batch_size: u64) {
        let rows_written = self.splice_stats.num_rows + self.current_row_group_stats.num_rows;
        if rows_written != row_count {
            let prior_num_rows = row_count - batch_size;
            panic!("In Struct column {}, the number of values written ({}) does not match the batch size ({})", 
                self.column_id, rows_written - prior_num_rows, batch_size);
        }

        for child in &self.children {
            child.verify_row_count(row_count, batch_size);
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
        self.current_row_group_stats = StructStatistics::new();
        self.row_group_stats = vec![];
        for child in &mut self.children {
            child.reset();
        }
    }
}


pub enum Data<'a> {
    Long(LongData<'a>),
    Struct(StructData<'a>)
}

impl<'a> Data<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        match schema {
            Schema::Short | Schema::Int | Schema::Long => Data::Long(LongData::new(schema, config, column_id)),
            Schema::Struct(fields) => Data::Struct(StructData::new(fields, config, column_id)),
        }
    }
}

// We could use `enum_dispatch` to autogenerate this boilerplate, but unfortunately it doesn't work with RLS.
impl<'a> BaseData<'a> for Data<'a> {
    fn column_id(&self) -> u32 {
        match self {
            Data::Long(x) => x.column_id(),
            Data::Struct(x) => x.column_id(),
        }
    }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        match self {
            Data::Long(x) => x.write_index_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_index_streams(out, stream_infos_out),
        }
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        match self {
            Data::Long(x) => x.write_data_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_data_streams(out, stream_infos_out),
        }
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        match self {
            Data::Long(x) => x.column_encodings(out),
            Data::Struct(x) => x.column_encodings(out),
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        match self {
            Data::Long(x) => x.statistics(out),
            Data::Struct(x) => x.statistics(out),
        }
    }

    fn verify_row_count(&self, row_count: u64, batch_size: u64) {
        match self {
            Data::Long(x) => x.verify_row_count(row_count, batch_size),
            Data::Struct(x) => x.verify_row_count(row_count, batch_size),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            Data::Long(x) => x.estimated_size(),
            Data::Struct(x) => x.estimated_size(),
        }
    }

    fn reset(&mut self) {
        match self {
            Data::Long(x) => x.reset(),
            Data::Struct(x) => x.reset(),
        }
    }
}
