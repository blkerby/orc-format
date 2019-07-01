use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, BooleanRLEPosition, UnsignedIntRLEv1, IntRLEv1Position};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, GenericStatistics};
use crate::writer::data::common::{BaseData, write_index};
use crate::writer::data::{GenericData, Data};

pub struct MapData {
    column_id: u32,
    pub(crate) keys: Box<Data>,
    pub(crate) values: Box<Data>,
    streams: MapDataStreams,
    stripe_stats: GenericStatistics,
    num_child_values: u64,
    row_group_stats: GenericStatistics,
    row_group_position: MapDataPosition,
    row_index_entries: Vec<MapRowIndexEntry>,
    config: Config,
}

struct MapDataStreams {
    present: BooleanRLE,
    lengths: UnsignedIntRLEv1,
}

#[derive(Copy, Clone)]
struct MapDataPosition {
    present: BooleanRLEPosition,
    lengths: IntRLEv1Position,
}

struct MapRowIndexEntry {
    position: MapDataPosition,
    stats: GenericStatistics,
}

impl MapDataPosition {
    pub fn record(&self, include_present: bool, out: &mut Vec<u64>) {
        if include_present {
            self.present.record(out);
        }
        self.lengths.record(out);
    }
}

impl MapDataStreams {
    pub fn position(&self) -> MapDataPosition {
        MapDataPosition {
            present: self.present.position(),
            lengths: self.lengths.position(),
        }
    }
}


impl MapData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        if let Schema::Map(key_schema, value_schema) = schema {        
            let streams = MapDataStreams {
                present: BooleanRLE::new(&config.compression),
                lengths: UnsignedIntRLEv1::new(&config.compression),                
            };
            Self {
                column_id: cid,
                keys: Box::new(Data::new(&key_schema, config, column_id)),
                values: Box::new(Data::new(&value_schema, config, column_id)),
                stripe_stats: GenericStatistics::new(),
                num_child_values: 0,
                row_group_stats: GenericStatistics::new(),
                row_group_position: streams.position(),
                row_index_entries: Vec::new(),
                config: config.clone(),
                streams,
            }
        } else { unreachable!() }
    }
    
    pub fn keys(&mut self) -> &mut Data {
        &mut self.keys
    }

    pub fn values(&mut self) -> &mut Data {
        &mut self.values
    }

    pub fn children(&mut self) -> (&mut Data, &mut Data) {
        (&mut self.keys, &mut self.values)
    }

    fn check_row_group(&mut self) {
        if self.row_group_stats.num_values == self.config.row_index_stride as u64 {
            self.finish_row_group();
        }
    }

    fn finish_row_group(&mut self) {
        if self.row_group_stats.num_values > 0 {
            self.stripe_stats.merge(&self.row_group_stats);
            self.row_index_entries.push(MapRowIndexEntry {
                position: self.row_group_position,
                stats: self.row_group_stats,
            });
            self.row_group_position = self.streams.position();
            self.row_group_stats = GenericStatistics::new();
        }
    }

    pub fn write(&mut self, len: u64) {
        self.streams.present.write(true);
        self.streams.lengths.write(len);
        self.num_child_values += len;
        self.row_group_stats.update();
        self.check_row_group();
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl GenericData for MapData {
    fn write_null(&mut self) {
        self.streams.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}

impl BaseData for MapData {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        self.finish_row_group();

        let mut row_index_entries: Vec<orc_proto::RowIndexEntry> = Vec::new();
        for entry in &self.row_index_entries {
            let mut row_index_entry = orc_proto::RowIndexEntry::new();
            let mut positions: Vec<u64> = Vec::new();
            entry.position.record(self.stripe_stats.has_null(), &mut positions);
            row_index_entry.set_positions(positions);
            row_index_entry.set_statistics(Statistics::Generic(entry.stats).to_proto());
            row_index_entries.push(row_index_entry);
        }
        write_index(row_index_entries, self.column_id(), &self.config.compression, out, stream_infos_out)?;

        self.keys.write_index_streams(out, stream_infos_out)?;
        self.values.write_index_streams(out, stream_infos_out)?;
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

        let lengths_start_pos = out.pos();
        self.streams.lengths.finish(out)?;
        let lengths_len = (out.pos() - lengths_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::LENGTH,
            column_id: self.column_id,
            length: lengths_len,
        });

        self.keys.write_data_streams(out, stream_infos_out)?;
        self.values.write_data_streams(out, stream_infos_out)?;

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        self.keys.column_encodings(out);
        self.values.column_encodings(out);
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Generic(self.stripe_stats));
        self.keys.statistics(out);
        self.values.statistics(out);
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Map), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }
        self.keys.verify_row_count(self.num_child_values);
        self.values.verify_row_count(self.num_child_values);
    }

    fn estimated_size(&self) -> usize {
        self.streams.present.estimated_size() + self.streams.lengths.estimated_size() + 
            self.keys.estimated_size() + self.values.estimated_size()
    }
}
