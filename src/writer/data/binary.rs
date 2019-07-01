use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::compression::{CompressionStream, CompressionStreamPosition};
use crate::writer::encoder::{BooleanRLE, BooleanRLEPosition, UnsignedIntRLEv1, IntRLEv1Position};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, BinaryStatistics};
use crate::writer::data::common::{BaseData, GenericData, write_index};


pub struct BinaryData {
    pub(crate) column_id: u32,
    streams: BinaryDataStreams,
    stripe_stats: BinaryStatistics,
    row_group_stats: BinaryStatistics,
    row_group_position: BinaryDataPosition,
    row_index_entries: Vec<BinaryRowIndexEntry>,
    config: Config,
}

struct BinaryDataStreams {
    present: BooleanRLE,
    data: CompressionStream,
    lengths: UnsignedIntRLEv1,
}

#[derive(Copy, Clone)]
struct BinaryDataPosition {
    present: BooleanRLEPosition,
    data: CompressionStreamPosition,
    lengths: IntRLEv1Position,
}

struct BinaryRowIndexEntry {
    position: BinaryDataPosition,
    stats: BinaryStatistics,
}

impl BinaryDataPosition {
    pub fn record(&self, include_present: bool, out: &mut Vec<u64>) {
        if include_present {
            self.present.record(out);
        }
        self.data.record(out);
        self.lengths.record(out);
    }
}

impl BinaryDataStreams {
    pub fn position(&self) -> BinaryDataPosition {
        BinaryDataPosition {
            present: self.present.position(),
            data: self.data.position(),
            lengths: self.lengths.position(),
        }
    }
}

impl BinaryData {
    pub(crate) fn new(config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        let streams = BinaryDataStreams {
            present: BooleanRLE::new(&config.compression),
            data: CompressionStream::new(&config.compression),
            lengths: UnsignedIntRLEv1::new(&config.compression),
        };
        Self {
            column_id: cid,
            stripe_stats: BinaryStatistics::new(),
            row_group_stats: BinaryStatistics::new(),
            row_group_position: streams.position(),
            row_index_entries: Vec::new(),
            config: config.clone(),
            streams,
        }
    }

    pub fn write(&mut self, x: &[u8]) {
        self.streams.present.write(true);
        self.streams.data.write_bytes(x);
        self.streams.lengths.write(x.len() as u64);
        self.row_group_stats.update(x);
        self.check_row_group();
    }

    fn check_row_group(&mut self) {
        if self.row_group_stats.num_values == self.config.row_index_stride as u64 {
            self.finish_row_group();
        }
    }

    fn finish_row_group(&mut self) {
        if self.row_group_stats.num_values > 0 {
            self.stripe_stats.merge(&self.row_group_stats);
            self.row_index_entries.push(BinaryRowIndexEntry {
                position: self.row_group_position,
                stats: self.row_group_stats,
            });
            self.row_group_position = self.streams.position();
            self.row_group_stats = BinaryStatistics::new();
        }
    }
}

impl GenericData for BinaryData {
    fn write_null(&mut self) {
        self.streams.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}

impl BaseData for BinaryData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        self.finish_row_group();
        let mut row_index_entries: Vec<orc_proto::RowIndexEntry> = Vec::new();
        for entry in &self.row_index_entries {
            let mut row_index_entry = orc_proto::RowIndexEntry::new();
            let mut positions: Vec<u64> = Vec::new();
            entry.position.record(self.stripe_stats.has_null(), &mut positions);
            row_index_entry.set_positions(positions);
            row_index_entry.set_statistics(Statistics::Binary(entry.stats).to_proto());
            row_index_entries.push(row_index_entry);
        }
        write_index(row_index_entries, self.column_id(), &self.config.compression, out, stream_infos_out)?;
        Ok(())
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        if self.stripe_stats.has_null() {
            let present_start_pos = out.pos();
            self.streams.present.finish(out)?;
            let present_len = (out.pos() - present_start_pos) as u64;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len as u64,
            });
        }
        
        let data_start_pos = out.pos();
        self.streams.data.finish(out)?;
        let data_len = (out.pos() - data_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: data_len,
        });

        let lengths_start_pos = out.pos();
        self.streams.lengths.finish(out)?;
        let lengths_len = (out.pos() - lengths_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::LENGTH,
            column_id: self.column_id,
            length: lengths_len,
        });
        
        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Binary(self.stripe_stats.clone()));
    }

    fn estimated_size(&self) -> usize {
        self.streams.present.estimated_size() + self.streams.data.estimated_size() + 
            self.streams.lengths.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Binary), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }
}
