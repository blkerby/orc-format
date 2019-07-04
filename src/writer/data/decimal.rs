use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::compression::{CompressionStream, CompressionStreamPosition};
use crate::writer::encoder::{BooleanRLE, BooleanRLEPosition, SignedIntRLEv1, IntRLEv1Position, VarInt};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, DecimalStatistics};
use crate::writer::data::common::{GenericData, BaseData, write_index};


pub struct DecimalData {
    pub(crate) column_id: u32,
    precision: u32,
    scale: u32,
    streams: DecimalDataStreams,
    stripe_stats: DecimalStatistics,
    row_group_stats: DecimalStatistics,
    row_group_position: DecimalDataPosition,
    row_index_entries: Vec<DecimalRowIndexEntry>,
    config: Config,
}

struct DecimalDataStreams {
    present: BooleanRLE,
    data: CompressionStream,
    // Only the constant 'scale' is repeatedly written to this. It is only included to satisfy 
    // ORC v1 spec (should no longer be needed in ORC v2).
    secondary_scale: SignedIntRLEv1,  
}

#[derive(Copy, Clone)]
struct DecimalDataPosition {
    present: BooleanRLEPosition,
    data: CompressionStreamPosition,
    secondary_scale: IntRLEv1Position,
}

struct DecimalRowIndexEntry {
    position: DecimalDataPosition,
    stats: DecimalStatistics,
}

impl DecimalDataPosition {
    pub fn record(&self, include_present: bool, out: &mut Vec<u64>) {
        if include_present {
            self.present.record(out);
        }
        self.data.record(out);
        self.secondary_scale.record(out);
    }
}

impl DecimalDataStreams {
    pub fn position(&self) -> DecimalDataPosition {
        DecimalDataPosition {
            present: self.present.position(),
            data: self.data.position(),
            secondary_scale: self.secondary_scale.position(),
        }
    }
}

impl DecimalData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        if let Schema::Decimal(precision, scale) = schema {
            let streams = DecimalDataStreams {
                present: BooleanRLE::new(&config.compression),
                data: CompressionStream::new(&config.compression),
                secondary_scale: SignedIntRLEv1::new(&config.compression),
            };
            Self {
                column_id: cid,
                precision: *precision,
                scale: *scale,
                stripe_stats: DecimalStatistics::new(*scale),
                row_group_stats: DecimalStatistics::new(*scale),
                row_group_position: streams.position(),
                row_index_entries: Vec::new(),
                config: config.clone(),
                streams,
            }
        } else { unreachable!() }
    }

    fn check_row_group(&mut self) {
        if self.row_group_stats.num_values == self.config.row_index_stride as u64 {
            self.finish_row_group();
        }
    }

    fn finish_row_group(&mut self) {
        if self.row_group_stats.num_values > 0 {
            self.stripe_stats.merge(&self.row_group_stats);
            self.row_index_entries.push(DecimalRowIndexEntry {
                position: self.row_group_position,
                stats: self.row_group_stats,
            });
            self.row_group_position = self.streams.position();
            self.row_group_stats = DecimalStatistics::new(self.scale);
        }
    }

    pub fn write_i64(&mut self, x: i64) {
        self.streams.present.write(true);
        x.write_varint(&mut self.streams.data);
        self.streams.secondary_scale.write(self.scale as i64);
        self.row_group_stats.update(x as i128);
        self.check_row_group();
    }

    pub fn write_i128(&mut self, x: i128) {
        self.streams.present.write(true);
        x.write_varint(&mut self.streams.data);
        self.streams.secondary_scale.write(self.scale as i64);
        self.row_group_stats.update(x);
        self.check_row_group();
    }

    pub fn precision(&self) -> u32 { self.precision }

    pub fn scale(&self) -> u32 { self.scale }
}

impl GenericData for DecimalData {
    fn write_null(&mut self) {
        self.streams.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}

impl BaseData for DecimalData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        self.finish_row_group();
        let mut row_index_entries: Vec<orc_proto::RowIndexEntry> = Vec::new();
        for entry in &self.row_index_entries {
            let mut row_index_entry = orc_proto::RowIndexEntry::new();
            let mut positions: Vec<u64> = Vec::new();
            entry.position.record(self.stripe_stats.has_null(), &mut positions);
            row_index_entry.set_positions(positions);
            row_index_entry.set_statistics(Statistics::Decimal(entry.stats).to_proto());
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
                length: present_len,
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

        let secondary_start_pos = out.pos();
        self.streams.secondary_scale.finish(out)?;
        let secondary_len = (out.pos() - secondary_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::SECONDARY,
            column_id: self.column_id,
            length: secondary_len,
        });

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Decimal(self.stripe_stats));
    }

    fn estimated_size(&self) -> usize {
        self.streams.present.estimated_size() + self.streams.data.estimated_size() 
            + self.streams.secondary_scale.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Decimal), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }
}
