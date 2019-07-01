use std::io::{Write, Result};
use protobuf::{CodedOutputStream, Message, RepeatedField};

use crate::protos::orc_proto;
use crate::writer::count_write::CountWrite;
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::Statistics;
use crate::writer::compression::{Compression, CompressionStream};

pub trait GenericData {
    fn write_null(&mut self);
}

pub trait BaseData: GenericData {
    fn column_id(&self) -> u32;
    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()>;
    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()>;
    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>);
    fn statistics(&self, out: &mut Vec<Statistics>);
    fn verify_row_count(&self, num_rows: u64);
    fn estimated_size(&self) -> usize;
}

pub fn write_index<W: Write>(
        entries: Vec<orc_proto::RowIndexEntry>, 
        column_id: u32,
        compression: &Compression,
        out: &mut CountWrite<W>, 
        stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
    let index_start_pos = out.pos();

    let mut compression_stream = CompressionStream::new(compression);
    let mut coded_out = CodedOutputStream::new(&mut compression_stream);
    let mut row_index = orc_proto::RowIndex::new();
    row_index.set_entry(RepeatedField::from_vec(entries));
    row_index.write_to(&mut coded_out)?;
    coded_out.flush()?;
    compression_stream.finish(out)?;
    
    let index_len = (out.pos() - index_start_pos) as u64;
    stream_infos_out.push(StreamInfo {
        kind: orc_proto::Stream_Kind::ROW_INDEX,
        column_id,
        length: index_len,
    });

    Ok(())
}
