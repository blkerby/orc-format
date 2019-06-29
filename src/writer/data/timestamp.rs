use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, SignedIntRLEv1, UnsignedIntRLEv1};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, TimestampStatistics};
use crate::writer::data::common::{GenericData, BaseData};


pub struct TimestampData {
    pub(crate) column_id: u32,
    present: BooleanRLE,
    seconds: SignedIntRLEv1,
    nanos: UnsignedIntRLEv1,
    stripe_stats: TimestampStatistics,
}

impl TimestampData {
    /// Number of seconds between UNIX epoch and the ORC timestamp origin (2015-01-01)
    pub const EPOCH_SECONDS: i64 = -1420070400; 

    pub(crate) fn new(config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        Self {
            column_id: cid,
            present: BooleanRLE::new(&config.compression),
            seconds: SignedIntRLEv1::new(&config.compression),
            nanos: UnsignedIntRLEv1::new(&config.compression),
            stripe_stats: TimestampStatistics::new(),
        }
    }

    pub fn write_nanos(&mut self, sec_epoch: i64, nanos: u32) {
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

        self.present.write(true);
        self.seconds.write(sec_epoch);
        self.nanos.write((nanos_val << 3 | trailing_zeros) as u64);
        self.stripe_stats.update(sec_epoch * 1000 + (nanos / 1000000) as i64);
    }
}

impl GenericData for TimestampData {
    fn write_null(&mut self) {
        self.present.write(false);
        self.stripe_stats.update_null();
    }
}

impl BaseData for TimestampData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut CountWrite<W>, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        Ok(())
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        if self.stripe_stats.has_null() {
            let present_start_pos = out.pos();
            self.present.finish(out)?;
            let present_len = (out.pos() - present_start_pos) as u64;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len,
            });
        }

        let seconds_start_pos = out.pos();
        self.seconds.finish(out)?;
        let seconds_len = (out.pos() - seconds_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: seconds_len,
        });

        let nanos_start_pos = out.pos();
        self.nanos.finish(out)?;
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
        self.present.estimated_size() + self.seconds.estimated_size() + self.nanos.estimated_size()
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Timestamp), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
    }

    fn reset(&mut self) {
        self.stripe_stats = TimestampStatistics::new();
    }
}
