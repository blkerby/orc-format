use crate::protos::orc_proto;

pub use common::BaseStatistics;
pub use generic::GenericStatistics;
pub use boolean::BooleanStatistics;
pub use long::LongStatistics;
pub use string::StringStatistics;
pub use double::DoubleStatistics;
pub use decimal64::Decimal64Statistics;
pub use timestamp::TimestampStatistics;
pub use binary::BinaryStatistics;

mod common;
mod boolean;
mod long;
mod generic;
mod string;
mod double;
mod decimal64;
mod timestamp;
mod binary;

#[derive(Debug, Clone)]
pub enum Statistics {
    Boolean(BooleanStatistics),
    Long(LongStatistics),
    Double(DoubleStatistics),
    Decimal64(Decimal64Statistics),
    Timestamp(TimestampStatistics),
    String(StringStatistics),
    Binary(BinaryStatistics),
    Generic(GenericStatistics),
}

impl Statistics {
    pub fn unwrap_boolean(&self) -> &BooleanStatistics { 
        if let Statistics::Boolean(x) = self { x } else { panic!("invalid argument to unwrap_boolean"); }
    }

    pub fn unwrap_long(&self) -> &LongStatistics { 
        if let Statistics::Long(x) = self { x } else { panic!("invalid argument to unwrap_long"); }
    }

    pub fn unwrap_decimal64(&self) -> &Decimal64Statistics { 
        if let Statistics::Decimal64(x) = self { x } else { panic!("invalid argument to unwrap_decimal64"); }
    }

    pub fn unwrap_string(&self) -> &StringStatistics { 
        if let Statistics::String(x) = self { x } else { panic!("invalid argument to unwrap_string"); }
    }

    pub fn unwrap_binary(&self) -> &BinaryStatistics { 
        if let Statistics::Binary(x) = self { x } else { panic!("invalid argument to unwrap_binary"); }
    }

    pub fn unwrap_double(&self) -> &DoubleStatistics { 
        if let Statistics::Double(x) = self { x } else { panic!("invalid argument to unwrap_double"); }
    }

    pub fn unwrap_timestamp(&self) -> &TimestampStatistics { 
        if let Statistics::Timestamp(x) = self { x } else { panic!("invalid argument to unwrap_timestamp"); }
    }

    pub fn unwrap_generic(&self) -> &GenericStatistics { 
        if let Statistics::Generic(x) = self { x } else { panic!("invalid argument to unwrap_struct"); }
    }

    pub fn to_proto(&self) -> orc_proto::ColumnStatistics {
        let mut stat = orc_proto::ColumnStatistics::new();
        stat.set_numberOfValues(self.num_present());
        stat.set_hasNull(self.has_null());
        match self {
            Statistics::Boolean(b) => {
                let mut bool_stat = orc_proto::BucketStatistics::new();
                bool_stat.set_count(vec![b.num_true, b.num_false]);
                stat.set_bucketStatistics(bool_stat);
            }
            Statistics::Long(long_statistics) => {
                let mut int_stat = orc_proto::IntegerStatistics::new();
                if let Some(x) = long_statistics.min { int_stat.set_minimum(x); }
                if let Some(x) = long_statistics.max { int_stat.set_maximum(x); }
                if let Some(x) = long_statistics.sum { int_stat.set_sum(x); }
                stat.set_intStatistics(int_stat);
            }
            Statistics::Decimal64(d) => {
                let mut dec_stat = orc_proto::DecimalStatistics::new();
                if let Some(x) = d.min { dec_stat.set_minimum(d.format(x)); }
                if let Some(x) = d.max { dec_stat.set_maximum(d.format(x)); }
                if let Some(x) = d.sum { dec_stat.set_sum(d.format(x)); }
                stat.set_decimalStatistics(dec_stat);
            }
            Statistics::Timestamp(d) => {
                let mut ts_stat = orc_proto::TimestampStatistics::new();
                if let Some(x) = d.min_epoch_millis { ts_stat.set_minimum(x); }
                if let Some(x) = d.max_epoch_millis { ts_stat.set_maximum(x); }
                stat.set_timestampStatistics(ts_stat);
            }
            Statistics::Double(double_statistics) => {
                let mut double_stat = orc_proto::DoubleStatistics::new();
                if let Some(x) = double_statistics.min { double_stat.set_minimum(x); }
                if let Some(x) = double_statistics.max { double_stat.set_maximum(x); }
                double_stat.set_sum(double_statistics.sum);
                stat.set_doubleStatistics(double_stat);
            }
            Statistics::String(string_statistics) => {
                let mut str_stat = orc_proto::StringStatistics::new();
                if let Some(x) = &string_statistics.min { str_stat.set_minimum(x.clone()); }
                if let Some(x) = &string_statistics.max { str_stat.set_maximum(x.clone()); }
                str_stat.set_sum(string_statistics.sum_lengths as i64);
                stat.set_stringStatistics(str_stat);
            }
            Statistics::Binary(binary_statistics) => {
                let mut bin_stat = orc_proto::BinaryStatistics::new();
                bin_stat.set_sum(binary_statistics.sum_lengths as i64);
                stat.set_binaryStatistics(bin_stat);
            }
            Statistics::Generic(_) => {}
        }
        stat
    }
}

impl BaseStatistics for Statistics {
    fn update_null(&mut self) {
        match self {
            Statistics::Boolean(x) => x.update_null(),
            Statistics::Long(x) => x.update_null(),
            Statistics::Double(x) => x.update_null(),
            Statistics::Decimal64(x) => x.update_null(),
            Statistics::Timestamp(x) => x.update_null(),
            Statistics::String(x) => x.update_null(),
            Statistics::Binary(x) => x.update_null(),
            Statistics::Generic(x) => x.update_null(),
        }
    }

    fn num_values(&self) -> u64 {
        match self {
            Statistics::Boolean(x) => x.num_values(),
            Statistics::Long(x) => x.num_values(),
            Statistics::Double(x) => x.num_values(),
            Statistics::Decimal64(x) => x.num_values(),
            Statistics::Timestamp(x) => x.num_values(),
            Statistics::String(x) => x.num_values(),
            Statistics::Binary(x) => x.num_values(),
            Statistics::Generic(x) => x.num_values(),
        }
    }

    fn num_present(&self) -> u64 {
        match self {
            Statistics::Boolean(x) => x.num_present(),
            Statistics::Long(x) => x.num_present(),
            Statistics::Double(x) => x.num_present(),
            Statistics::Decimal64(x) => x.num_present(),
            Statistics::Timestamp(x) => x.num_present(),
            Statistics::String(x) => x.num_present(),
            Statistics::Binary(x) => x.num_present(),
            Statistics::Generic(x) => x.num_present(),
        }
    }

    fn merge(&mut self, rhs: &Statistics) {
        match self {
            Statistics::Boolean(x) => x.merge(rhs.unwrap_boolean()),
            Statistics::Long(x) => x.merge(rhs.unwrap_long()),
            Statistics::Double(x) => x.merge(rhs.unwrap_double()),
            Statistics::Decimal64(x) => x.merge(rhs.unwrap_decimal64()),
            Statistics::Timestamp(x) => x.merge(rhs.unwrap_timestamp()),
            Statistics::String(x) => x.merge(rhs.unwrap_string()),
            Statistics::Binary(x) => x.merge(rhs.unwrap_binary()),
            Statistics::Generic(x) => x.merge(rhs.unwrap_generic()),
        }
    }
}
