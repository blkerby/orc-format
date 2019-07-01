use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, BooleanRLEPosition};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, GenericStatistics};
use crate::writer::data::common::{GenericData, BaseData, write_index};
use crate::writer::data::Data;

pub struct StructData {
    column_id: u32,
    pub(crate) children: Vec<Data>,
    streams: StructDataStreams,
    stripe_stats: GenericStatistics,
    field_names: Vec<String>,
    row_group_stats: GenericStatistics,
    row_group_position: StructDataPosition,
    row_index_entries: Vec<StructRowIndexEntry>,
    config: Config,
}


struct StructDataStreams {
    present: BooleanRLE,
}

#[derive(Copy, Clone)]
struct StructDataPosition {
    present: BooleanRLEPosition,
}

struct StructRowIndexEntry {
    position: StructDataPosition,
    stats: GenericStatistics,
}

impl StructDataPosition {
    pub fn record(&self, include_present: bool, out: &mut Vec<u64>) {
        if include_present {
            self.present.record(out);
        }
    }
}

impl StructDataStreams {
    pub fn position(&self) -> StructDataPosition {
        StructDataPosition {
            present: self.present.position(),
        }
    }
}


impl StructData {
    pub(crate) fn new(schema: &Schema, config: &Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        let mut children: Vec<Data> = Vec::new();
        let mut field_names: Vec<String> = Vec::new();
        *column_id += 1;

        if let Schema::Struct(fields) = schema {
            for field in fields {
                field_names.push(field.0.clone());
                children.push(Data::new(&field.1, config, column_id));
            }

            let streams = StructDataStreams {
                present: BooleanRLE::new(&config.compression),
            };
            Self {
                column_id: cid,
                children: children,
                field_names,
                stripe_stats: GenericStatistics::new(),
                row_group_stats: GenericStatistics::new(),
                row_group_position: streams.position(),
                row_index_entries: Vec::new(),
                config: config.clone(),
                streams,
            }
        } else { unreachable!() }
    }
    
    pub(crate) fn field_names(&self) -> &[String] {
        &self.field_names
    }

    pub fn children(&mut self) -> &mut [Data] {
        &mut self.children
    }

    pub fn child(&mut self, i: usize) -> &mut Data {
        &mut self.children[i]
    }

    pub fn write(&mut self) {
        self.streams.present.write(true);
        self.row_group_stats.update();
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
            self.row_index_entries.push(StructRowIndexEntry {
                position: self.row_group_position,
                stats: self.row_group_stats,
            });
            self.row_group_position = self.streams.position();
            self.row_group_stats = GenericStatistics::new();
        }
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl GenericData for StructData {
    fn write_null(&mut self) {
        self.streams.present.write(false);
        self.row_group_stats.update_null();
        self.check_row_group();
    }
}

impl BaseData for StructData {
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
        
        for child in &mut self.children {
            child.write_index_streams(out, stream_infos_out)?;
        }
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
        
        for child in &mut self.children {
            child.write_data_streams(out, stream_infos_out)?;
        }

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        for child in &self.children {
            child.column_encodings(out);
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Generic(self.stripe_stats));
        for child in &self.children {
            child.statistics(out);
        }
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values() + self.row_group_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Struct), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }

        for child in &self.children {
            child.verify_row_count(self.stripe_stats.num_present());
        }
    }

    fn estimated_size(&self) -> usize {
        let mut size = self.streams.present.estimated_size();
        for child in &self.children {
            size += child.estimated_size();
        }
        size
    }
}
