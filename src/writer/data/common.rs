use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::count_write::CountWrite;
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::Statistics;

pub trait BaseData<'a> {
    fn schema(&self) -> &'a Schema;
    fn column_id(&self) -> u32;
    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()>;
    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()>;
    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>);
    fn statistics(&self, out: &mut Vec<Statistics>);
    fn verify_row_count(&self, num_rows: u64);
    fn estimated_size(&self) -> usize;
    fn reset(&mut self);
}
