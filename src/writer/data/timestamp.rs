use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, BooleanRLEPosition, SignedIntRLEv1, UnsignedIntRLEv1, IntRLEv1Position};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, TimestampStatistics};
use crate::writer::data::common::{GenericData, BaseData, write_index};


pub struct TimestampData {
    pub(crate) column_id: u32,
    streams: TimestampDataStreams,
    stripe_stats: TimestampStatistics,
    row_group_stats: TimestampStatistics,
    row_group_position: TimestampDataPosition,
    row_index_entries: Vec<TimestampRowIndexEntry>,
    config: Config,
}

struct TimestampDataStreams {
    present: BooleanRLE,
    seconds: SignedIntRLEv1,
    nanos: UnsignedIntRLEv1,
}

#[derive(Copy, Clone)]
struct TimestampDataPosition {
    present: BooleanRLEPosition,
    seconds: IntRLEv1Position,
    nanos: IntRLEv1Position,
}

struct TimestampRowIndexEntry {
    position: TimestampDataPosition,
    stats: TimestampStatistics,
}

impl TimestampDataPosition {
    pub fn record(&self, include_present: bool, out: &mut Vec<u64>) {
        if include_present {
            self.present.record(out);
        }
        self.seconds.record(out);
        self.nanos.record(out);
    }
}

impl TimestampDataStreams {
    pub fn position(&self) -> TimestampDataPosition {
        TimestampDataPosition {
            present: self.present.position(),
            seconds: self.seconds.position(),
            nanos: self.nanos.position(),
        }
    }
}


impl TimestampData {
    /// Number of seconds between UNIX epoch and the ORC timestamp origin (2015-01-01)
    pub const EPOCH_SECONDS: i64 = -1420070400; 

    pub(crate) fn new(config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        let streams = TimestampDataStreams {
            present: BooleanRLE::new(&config.compression),
            seconds: SignedIntRLEv1::new(&config.compression),
            nanos: UnsignedIntRLEv1::new(&config.compression),
        };
        Self {
            column_id: cid,
            stripe_stats: TimestampStatistics::new(),
            row_group_stats: TimestampStatistics::new(),
            row_group_position: streams.position(),
            row_index_entries: Vec::new(),
            config: config.clone(),
            streams,
        }
    }

    fn check_row_group(&mut self) {
        if self.row_group_stats.num_values == self.config.row_index_stride as u64 {
            self.finish_row_group();
        }
    }

    fn finish_row_group(&mut self) {
        if self.row_group_stats.num_values > 0 {
            self.stripe_stats.merge(&self.row_group_stats);
            self.row_index_entries.push(TimestampRowIndexEntry {
                position: self.row_group_position,
                stats: self.row_group_stats,
            });
            self.row_group_position = self.streams.position();
            self.row_group_stats = TimestampStatistics::new();
        }
    }

    
    pub fn write_nanos_epoch(&mut self, sec_epoch: i64, nanos: u32) {
        self.write_nanos(sec_epoch + Self::EPOCH_SECONDS, nanos);
    }

    pub fn write_nanos(&mut self, sec: i64, nanos: u32) {
        let mut trailing_zeros = 0;
        let mut nanos_val = nanos;

        if nanos_val != 0 && nanos_val % 100 == 0 {
            trailing_zeros = 1;
            nanos_val /= 100;
            if nanos_val % 10000 == 0 {
                trailing_zeros += 4;
                nanos_val /= 10000;
            }
            if nanos_val % 100 == 0 {
                trailing_zeros += 2;
                nanos_val /= 100;
            }
            if nanos_val % 10 == 0 {
                trailing_zeros += 1;
                nanos_val /= 10;
            }
        }

        self.streams.present.write(true);
        self.streams.seconds.write(sec);
        self.streams.nanos.write((nanos_val << 3 | trailing_zeros) as u64);
        self.row_group_stats.update(sec * 1000 + (nanos / 1000000) as i64);
        self.check_row_group();
    }
}

impl GenericData for TimestampData {
    fn write_null(&mut self) {
        self.streams.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}

impl BaseData for TimestampData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        self.finish_row_group();
        let mut row_index_entries: Vec<orc_proto::RowIndexEntry> = Vec::new();
        for entry in &self.row_index_entries {
            let mut row_index_entry = orc_proto::RowIndexEntry::new();
            let mut positions: Vec<u64> = Vec::new();
            entry.position.record(self.stripe_stats.has_null(), &mut positions);
            row_index_entry.set_positions(positions);
            row_index_entry.set_statistics(Statistics::Timestamp(entry.stats).to_proto());
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

        let seconds_start_pos = out.pos();
        self.streams.seconds.finish(out)?;
        let seconds_len = (out.pos() - seconds_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: seconds_len,
        });

        let nanos_start_pos = out.pos();
        self.streams.nanos.finish(out)?;
        let nanos_len = (out.pos() - nanos_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::SECONDARY,
            column_id: self.column_id,
            length: nanos_len,
        });

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        out.push(Statistics::Timestamp(self.stripe_stats));
    }

    fn estimated_size(&self) -> usize {
        self.streams.present.estimated_size() 
            + self.streams.seconds.estimated_size() 
            + self.streams.nanos.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Timestamp), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }
}
