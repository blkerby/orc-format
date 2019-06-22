use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use super::Config;
use super::stripe::StreamInfo;
use super::statistics::Statistics;
use crate::writer::count_write::CountWrite;

pub use common::BaseData;
pub use boolean::BooleanData;
pub use long::LongData;
pub use float::FloatData;
pub use double::DoubleData;
pub use timestamp::TimestampData;
pub use decimal64::Decimal64Data;
pub use string::StringData;
pub use struct_::StructData;
pub use list::ListData;
pub use map::MapData;

mod common;
mod boolean;
mod long;
mod float;
mod double;
mod timestamp;
mod decimal64;
mod string;
mod struct_;
mod list;
mod map;

pub enum Data<'a> {
    Boolean(BooleanData<'a>),
    Long(LongData<'a>),
    Float(FloatData<'a>),
    Double(DoubleData<'a>),
    Timestamp(TimestampData<'a>),
    Decimal64(Decimal64Data<'a>),
    String(StringData<'a>),
    List(ListData<'a>),
    Struct(StructData<'a>),
    Map(MapData<'a>),
}

impl<'a> Data<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        match schema {
            Schema::Boolean => Data::Boolean(BooleanData::new(schema, config, column_id)),
            Schema::Short | Schema::Int | Schema::Long | Schema::Date => 
                Data::Long(LongData::new(schema, config, column_id)),
            Schema::Float => Data::Float(FloatData::new(schema, config, column_id)),
            Schema::Double => Data::Double(DoubleData::new(schema, config, column_id)),
            Schema::Timestamp => Data::Timestamp(TimestampData::new(schema, config, column_id)),
            Schema::Decimal(_, _) => Data::Decimal64(Decimal64Data::new(schema, config, column_id)),
            Schema::String => Data::String(StringData::new(schema, config, column_id)),
            Schema::Struct(_) => Data::Struct(StructData::new(schema, config, column_id)),
            Schema::List(_) => Data::List(ListData::new(schema, config, column_id)),
            Schema::Map(_, _) => Data::Map(MapData::new(schema, config, column_id)),
        }
    }

    pub fn unwrap_boolean(&mut self) -> &mut BooleanData<'a> {
        if let Data::Boolean(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_long(&mut self) -> &mut LongData<'a> {
        if let Data::Long(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_float(&mut self) -> &mut FloatData<'a> {
        if let Data::Float(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_double(&mut self) -> &mut DoubleData<'a> {
        if let Data::Double(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_timestamp(&mut self) -> &mut TimestampData<'a> {
        if let Data::Timestamp(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_decimal64(&mut self) -> &mut Decimal64Data<'a> {
        if let Data::Decimal64(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_string(&mut self) -> &mut StringData<'a> {
        if let Data::String(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_struct(&mut self) -> &mut StructData<'a> {
        if let Data::Struct(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_list(&mut self) -> &mut ListData<'a> {
        if let Data::List(x) = self { x } else { unreachable!() }
    }

    pub fn unwrap_map(&mut self) -> &mut MapData<'a> {
        if let Data::Map(x) = self { x } else { unreachable!() }
    }
}

// We could use `enum_dispatch` to autogenerate this boilerplate, but unfortunately it doesn't work with RLS.
impl<'a> BaseData<'a> for Data<'a> {
    fn schema(&self) -> &'a Schema {
        match self {
            Data::Boolean(x) => x.schema(),
            Data::Long(x) => x.schema(),
            Data::Float(x) => x.schema(),
            Data::Double(x) => x.schema(),
            Data::Timestamp(x) => x.schema(),
            Data::Decimal64(x) => x.schema(),
            Data::String(x) => x.schema(),
            Data::Struct(x) => x.schema(),
            Data::List(x) => x.schema(),
            Data::Map(x) => x.schema(),
        }
    }

    fn column_id(&self) -> u32 {
        match self {
            Data::Boolean(x) => x.column_id(),
            Data::Long(x) => x.column_id(),
            Data::Float(x) => x.column_id(),
            Data::Double(x) => x.column_id(),
            Data::Timestamp(x) => x.column_id(),
            Data::Decimal64(x) => x.column_id(),
            Data::String(x) => x.column_id(),
            Data::Struct(x) => x.column_id(),
            Data::List(x) => x.column_id(),
            Data::Map(x) => x.column_id(),
        }
    }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        match self {
            Data::Boolean(x) => x.write_index_streams(out, stream_infos_out),
            Data::Long(x) => x.write_index_streams(out, stream_infos_out),
            Data::Float(x) => x.write_index_streams(out, stream_infos_out),
            Data::Double(x) => x.write_index_streams(out, stream_infos_out),
            Data::Timestamp(x) => x.write_index_streams(out, stream_infos_out),
            Data::Decimal64(x) => x.write_index_streams(out, stream_infos_out),
            Data::String(x) => x.write_index_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_index_streams(out, stream_infos_out),
            Data::List(x) => x.write_index_streams(out, stream_infos_out),
            Data::Map(x) => x.write_index_streams(out, stream_infos_out),
        }
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        match self {
            Data::Boolean(x) => x.write_data_streams(out, stream_infos_out),
            Data::Long(x) => x.write_data_streams(out, stream_infos_out),
            Data::Float(x) => x.write_data_streams(out, stream_infos_out),
            Data::Double(x) => x.write_data_streams(out, stream_infos_out),
            Data::Timestamp(x) => x.write_data_streams(out, stream_infos_out),
            Data::Decimal64(x) => x.write_data_streams(out, stream_infos_out),
            Data::String(x) => x.write_data_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_data_streams(out, stream_infos_out),
            Data::List(x) => x.write_data_streams(out, stream_infos_out),
            Data::Map(x) => x.write_data_streams(out, stream_infos_out),
        }
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        match self {
            Data::Boolean(x) => x.column_encodings(out),
            Data::Long(x) => x.column_encodings(out),
            Data::Float(x) => x.column_encodings(out),
            Data::Double(x) => x.column_encodings(out),
            Data::Timestamp(x) => x.column_encodings(out),
            Data::Decimal64(x) => x.column_encodings(out),
            Data::String(x) => x.column_encodings(out),
            Data::Struct(x) => x.column_encodings(out),
            Data::List(x) => x.column_encodings(out),
            Data::Map(x) => x.column_encodings(out),
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        match self {
            Data::Boolean(x) => x.statistics(out),
            Data::Long(x) => x.statistics(out),
            Data::Float(x) => x.statistics(out),
            Data::Double(x) => x.statistics(out),
            Data::Timestamp(x) => x.statistics(out),
            Data::Decimal64(x) => x.statistics(out),
            Data::String(x) => x.statistics(out),
            Data::Struct(x) => x.statistics(out),
            Data::List(x) => x.statistics(out),
            Data::Map(x) => x.statistics(out),
        }
    }

    fn verify_row_count(&self, row_count: u64) {
        match self {
            Data::Boolean(x) => x.verify_row_count(row_count),
            Data::Long(x) => x.verify_row_count(row_count),
            Data::Float(x) => x.verify_row_count(row_count),
            Data::Double(x) => x.verify_row_count(row_count),
            Data::Timestamp(x) => x.verify_row_count(row_count),
            Data::Decimal64(x) => x.verify_row_count(row_count),
            Data::String(x) => x.verify_row_count(row_count),
            Data::Struct(x) => x.verify_row_count(row_count),
            Data::List(x) => x.verify_row_count(row_count),
            Data::Map(x) => x.verify_row_count(row_count),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            Data::Boolean(x) => x.estimated_size(),
            Data::Long(x) => x.estimated_size(),
            Data::Float(x) => x.estimated_size(),
            Data::Double(x) => x.estimated_size(),
            Data::Timestamp(x) => x.estimated_size(),
            Data::Decimal64(x) => x.estimated_size(),
            Data::String(x) => x.estimated_size(),
            Data::Struct(x) => x.estimated_size(),
            Data::List(x) => x.estimated_size(),
            Data::Map(x) => x.estimated_size(),
        }
    }

    fn reset(&mut self) {
        match self {
            Data::Boolean(x) => x.reset(),
            Data::Long(x) => x.reset(),
            Data::Float(x) => x.reset(),
            Data::Double(x) => x.reset(),
            Data::Timestamp(x) => x.reset(),
            Data::Decimal64(x) => x.reset(),
            Data::String(x) => x.reset(),
            Data::Struct(x) => x.reset(),
            Data::List(x) => x.reset(),
            Data::Map(x) => x.reset(),
        }
    }
}
