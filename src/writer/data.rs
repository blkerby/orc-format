use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use super::Config;
use super::stripe::StreamInfo;
use super::statistics::Statistics;

pub use common::BaseData;
pub use long::LongData;
pub use float::FloatData;
pub use double::DoubleData;
pub use string::StringData;
pub use struct_::StructData;

mod common;
mod long;
mod float;
mod double;
mod string;
mod struct_;

pub enum Data<'a> {
    Long(LongData<'a>),
    Float(FloatData<'a>),
    Double(DoubleData<'a>),
    String(StringData<'a>),
    Struct(StructData<'a>),
}

impl<'a> Data<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        match schema {
            Schema::Short | Schema::Int | Schema::Long | Schema::Date => 
                Data::Long(LongData::new(schema, config, column_id)),
            Schema::Float => Data::Float(FloatData::new(schema, config, column_id)),
            Schema::Double => Data::Double(DoubleData::new(schema, config, column_id)),
            Schema::String => Data::String(StringData::new(schema, config, column_id)),
            Schema::Struct(fields) => Data::Struct(StructData::new(fields, config, column_id)),
        }
    }
}

// We could use `enum_dispatch` to autogenerate this boilerplate, but unfortunately it doesn't work with RLS.
impl<'a> BaseData<'a> for Data<'a> {
    fn column_id(&self) -> u32 {
        match self {
            Data::Long(x) => x.column_id(),
            Data::Float(x) => x.column_id(),
            Data::Double(x) => x.column_id(),
            Data::String(x) => x.column_id(),
            Data::Struct(x) => x.column_id(),
        }
    }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        match self {
            Data::Long(x) => x.write_index_streams(out, stream_infos_out),
            Data::Float(x) => x.write_index_streams(out, stream_infos_out),
            Data::Double(x) => x.write_index_streams(out, stream_infos_out),
            Data::String(x) => x.write_index_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_index_streams(out, stream_infos_out),
        }
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<u64> {
        match self {
            Data::Long(x) => x.write_data_streams(out, stream_infos_out),
            Data::Float(x) => x.write_data_streams(out, stream_infos_out),
            Data::Double(x) => x.write_data_streams(out, stream_infos_out),
            Data::String(x) => x.write_data_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_data_streams(out, stream_infos_out),
        }
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        match self {
            Data::Long(x) => x.column_encodings(out),
            Data::Float(x) => x.column_encodings(out),
            Data::Double(x) => x.column_encodings(out),
            Data::String(x) => x.column_encodings(out),
            Data::Struct(x) => x.column_encodings(out),
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        match self {
            Data::Long(x) => x.statistics(out),
            Data::Float(x) => x.statistics(out),
            Data::Double(x) => x.statistics(out),
            Data::String(x) => x.statistics(out),
            Data::Struct(x) => x.statistics(out),
        }
    }

    fn verify_row_count(&self, row_count: u64) {
        match self {
            Data::Long(x) => x.verify_row_count(row_count),
            Data::Float(x) => x.verify_row_count(row_count),
            Data::Double(x) => x.verify_row_count(row_count),
            Data::String(x) => x.verify_row_count(row_count),
            Data::Struct(x) => x.verify_row_count(row_count),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            Data::Long(x) => x.estimated_size(),
            Data::Float(x) => x.estimated_size(),
            Data::Double(x) => x.estimated_size(),
            Data::String(x) => x.estimated_size(),
            Data::Struct(x) => x.estimated_size(),
        }
    }

    fn reset(&mut self) {
        match self {
            Data::Long(x) => x.reset(),
            Data::Float(x) => x.reset(),
            Data::Double(x) => x.reset(),
            Data::String(x) => x.reset(),
            Data::Struct(x) => x.reset(),
        }
    }
}
