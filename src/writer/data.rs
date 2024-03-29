use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use super::Config;
use super::stripe::StreamInfo;
use super::statistics::Statistics;
use crate::writer::count_write::CountWrite;

pub use common::{GenericData, BaseData};
pub use boolean::BooleanData;
pub use long::LongData;
pub use float::FloatData;
pub use double::DoubleData;
pub use timestamp::TimestampData;
pub use decimal::DecimalData;
pub use string::StringData;
pub use binary::BinaryData;
pub use struct_::StructData;
pub use list::ListData;
pub use map::MapData;
pub use union::UnionData;

mod common;
mod boolean;
mod long;
mod float;
mod double;
mod timestamp;
mod decimal;
mod string;
mod binary;
mod struct_;
mod list;
mod map;
mod union;

pub enum Data {
    Boolean(BooleanData),
    Long(LongData),
    Float(FloatData),
    Double(DoubleData),
    Timestamp(TimestampData),
    Decimal(DecimalData),
    String(StringData),
    Binary(BinaryData),
    List(ListData),
    Struct(StructData),
    Map(MapData),
    Union(UnionData),
}

impl Data {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        match schema {
            Schema::Boolean => Data::Boolean(BooleanData::new(config, column_id)),
            Schema::Short | Schema::Int | Schema::Long | Schema::Date => 
                Data::Long(LongData::new(schema, config, column_id)),
            Schema::Float => Data::Float(FloatData::new(config, column_id)),
            Schema::Double => Data::Double(DoubleData::new(config, column_id)),
            Schema::Timestamp => Data::Timestamp(TimestampData::new(config, column_id)),
            Schema::Decimal(_, _) => Data::Decimal(DecimalData::new(schema, config, column_id)),
            Schema::String | Schema::VarChar(_) | Schema::Char(_) => 
                Data::String(StringData::new(schema, config, column_id)),
            Schema::Binary => Data::Binary(BinaryData::new(config, column_id)),
            Schema::Struct(_) => Data::Struct(StructData::new(schema, config, column_id)),
            Schema::List(_) => Data::List(ListData::new(schema, config, column_id)),
            Schema::Map(_, _) => Data::Map(MapData::new(schema, config, column_id)),
            Schema::Union(_) => Data::Union(UnionData::new(schema, config, column_id)),
        }
    }

    pub fn unwrap_boolean(&mut self) -> &mut BooleanData {
        if let Data::Boolean(x) = self { x } else { panic!("unwrap_boolean called on incorrect type of data"); }
    }

    pub fn unwrap_long(&mut self) -> &mut LongData {
        if let Data::Long(x) = self { x } else { panic!("unwrap_long called on incorrect type of data"); }
    }

    pub fn unwrap_float(&mut self) -> &mut FloatData {
        if let Data::Float(x) = self { x } else { panic!("unwrap_float called on incorrect type of data"); }
    }

    pub fn unwrap_double(&mut self) -> &mut DoubleData {
        if let Data::Double(x) = self { x } else { panic!("unwrap_double called on incorrect type of data"); }
    }

    pub fn unwrap_timestamp(&mut self) -> &mut TimestampData {
        if let Data::Timestamp(x) = self { x } else { panic!("unwrap_timestamp called on incorrect type of data"); }
    }

    pub fn unwrap_decimal(&mut self) -> &mut DecimalData {
        if let Data::Decimal(x) = self { x } else { panic!("unwrap_decimal called on incorrect type of data"); }
    }

    pub fn unwrap_string(&mut self) -> &mut StringData {
        if let Data::String(x) = self { x } else { panic!("unwrap_string called on incorrect type of data"); }
    }

    pub fn unwrap_binary(&mut self) -> &mut BinaryData {
        if let Data::Binary(x) = self { x } else { panic!("unwrap_binary called on incorrect type of data"); }
    }

    pub fn unwrap_struct(&mut self) -> &mut StructData {
        if let Data::Struct(x) = self { x } else { panic!("unwrap_struct called on incorrect type of data"); }
    }

    pub fn unwrap_list(&mut self) -> &mut ListData {
        if let Data::List(x) = self { x } else { panic!("unwrap_list called on incorrect type of data"); }
    }

    pub fn unwrap_map(&mut self) -> &mut MapData {
        if let Data::Map(x) = self { x } else { panic!("unwrap_map called on incorrect type of data"); }
    }

    pub fn unwrap_union(&mut self) -> &mut UnionData {
        if let Data::Union(x) = self { x } else { panic!("unwrap_union called on incorrect type of data"); }
    }
}

impl GenericData for Data {
    fn write_null(&mut self) {
        match self {
            Data::Boolean(x) => x.write_null(),
            Data::Long(x) => x.write_null(),
            Data::Float(x) => x.write_null(),
            Data::Double(x) => x.write_null(),
            Data::Timestamp(x) => x.write_null(),
            Data::Decimal(x) => x.write_null(),
            Data::String(x) => x.write_null(),
            Data::Binary(x) => x.write_null(),
            Data::Struct(x) => x.write_null(),
            Data::List(x) => x.write_null(),
            Data::Map(x) => x.write_null(),
            Data::Union(x) => x.write_null(),
        }
    }
}

// We could use `enum_dispatch` to autogenerate this boilerplate, but unfortunately it doesn't work with RLS.
// Might be worthwhile to use a macro here ...
impl BaseData for Data {
    fn column_id(&self) -> u32 {
        match self {
            Data::Boolean(x) => x.column_id(),
            Data::Long(x) => x.column_id(),
            Data::Float(x) => x.column_id(),
            Data::Double(x) => x.column_id(),
            Data::Timestamp(x) => x.column_id(),
            Data::Decimal(x) => x.column_id(),
            Data::String(x) => x.column_id(),
            Data::Binary(x) => x.column_id(),
            Data::Struct(x) => x.column_id(),
            Data::List(x) => x.column_id(),
            Data::Map(x) => x.column_id(),
            Data::Union(x) => x.column_id(),
        }
    }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        match self {
            Data::Boolean(x) => x.write_index_streams(out, stream_infos_out),
            Data::Long(x) => x.write_index_streams(out, stream_infos_out),
            Data::Float(x) => x.write_index_streams(out, stream_infos_out),
            Data::Double(x) => x.write_index_streams(out, stream_infos_out),
            Data::Timestamp(x) => x.write_index_streams(out, stream_infos_out),
            Data::Decimal(x) => x.write_index_streams(out, stream_infos_out),
            Data::String(x) => x.write_index_streams(out, stream_infos_out),
            Data::Binary(x) => x.write_index_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_index_streams(out, stream_infos_out),
            Data::List(x) => x.write_index_streams(out, stream_infos_out),
            Data::Map(x) => x.write_index_streams(out, stream_infos_out),
            Data::Union(x) => x.write_index_streams(out, stream_infos_out),
        }
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        match self {
            Data::Boolean(x) => x.write_data_streams(out, stream_infos_out),
            Data::Long(x) => x.write_data_streams(out, stream_infos_out),
            Data::Float(x) => x.write_data_streams(out, stream_infos_out),
            Data::Double(x) => x.write_data_streams(out, stream_infos_out),
            Data::Timestamp(x) => x.write_data_streams(out, stream_infos_out),
            Data::Decimal(x) => x.write_data_streams(out, stream_infos_out),
            Data::String(x) => x.write_data_streams(out, stream_infos_out),
            Data::Binary(x) => x.write_data_streams(out, stream_infos_out),
            Data::Struct(x) => x.write_data_streams(out, stream_infos_out),
            Data::List(x) => x.write_data_streams(out, stream_infos_out),
            Data::Map(x) => x.write_data_streams(out, stream_infos_out),
            Data::Union(x) => x.write_data_streams(out, stream_infos_out),
        }
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        match self {
            Data::Boolean(x) => x.column_encodings(out),
            Data::Long(x) => x.column_encodings(out),
            Data::Float(x) => x.column_encodings(out),
            Data::Double(x) => x.column_encodings(out),
            Data::Timestamp(x) => x.column_encodings(out),
            Data::Decimal(x) => x.column_encodings(out),
            Data::String(x) => x.column_encodings(out),
            Data::Binary(x) => x.column_encodings(out),
            Data::Struct(x) => x.column_encodings(out),
            Data::List(x) => x.column_encodings(out),
            Data::Map(x) => x.column_encodings(out),
            Data::Union(x) => x.column_encodings(out),
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        match self {
            Data::Boolean(x) => x.statistics(out),
            Data::Long(x) => x.statistics(out),
            Data::Float(x) => x.statistics(out),
            Data::Double(x) => x.statistics(out),
            Data::Timestamp(x) => x.statistics(out),
            Data::Decimal(x) => x.statistics(out),
            Data::String(x) => x.statistics(out),
            Data::Binary(x) => x.statistics(out),
            Data::Struct(x) => x.statistics(out),
            Data::List(x) => x.statistics(out),
            Data::Map(x) => x.statistics(out),
            Data::Union(x) => x.statistics(out),
        }
    }

    fn verify_row_count(&self, row_count: u64) {
        match self {
            Data::Boolean(x) => x.verify_row_count(row_count),
            Data::Long(x) => x.verify_row_count(row_count),
            Data::Float(x) => x.verify_row_count(row_count),
            Data::Double(x) => x.verify_row_count(row_count),
            Data::Timestamp(x) => x.verify_row_count(row_count),
            Data::Decimal(x) => x.verify_row_count(row_count),
            Data::String(x) => x.verify_row_count(row_count),
            Data::Binary(x) => x.verify_row_count(row_count),
            Data::Struct(x) => x.verify_row_count(row_count),
            Data::List(x) => x.verify_row_count(row_count),
            Data::Map(x) => x.verify_row_count(row_count),
            Data::Union(x) => x.verify_row_count(row_count),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            Data::Boolean(x) => x.estimated_size(),
            Data::Long(x) => x.estimated_size(),
            Data::Float(x) => x.estimated_size(),
            Data::Double(x) => x.estimated_size(),
            Data::Timestamp(x) => x.estimated_size(),
            Data::Decimal(x) => x.estimated_size(),
            Data::String(x) => x.estimated_size(),
            Data::Binary(x) => x.estimated_size(),
            Data::Struct(x) => x.estimated_size(),
            Data::List(x) => x.estimated_size(),
            Data::Map(x) => x.estimated_size(),
            Data::Union(x) => x.estimated_size(),
        }
    }
}
