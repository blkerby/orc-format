use crate::protos::orc_proto;

pub use common::BaseStatistics;
pub use boolean::BooleanStatistics;
pub use long::LongStatistics;
pub use struct_::StructStatistics;
pub use string::StringStatistics;
pub use double::DoubleStatistics;
pub use decimal64::Decimal64Statistics;

mod common;
mod boolean;
mod long;
mod struct_;
mod string;
mod double;
mod decimal64;

#[derive(Debug, Clone)]
pub enum Statistics {
    Boolean(BooleanStatistics),
    Long(LongStatistics),
    Double(DoubleStatistics),
    Decimal64(Decimal64Statistics),
    String(StringStatistics),
    Struct(StructStatistics),
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

    pub fn unwrap_double(&self) -> &DoubleStatistics { 
        if let Statistics::Double(x) = self { x } else { panic!("invalid argument to unwrap_double"); }
    }

    pub fn unwrap_struct(&self) -> &StructStatistics { 
        if let Statistics::Struct(x) = self { x } else { panic!("invalid argument to unwrap_struct"); }
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
            Statistics::Struct(_s) => {}
        }
        stat
    }
}

impl BaseStatistics for Statistics {
    fn num_values(&self) -> u64 {
        match self {
            Statistics::Boolean(x) => x.num_values(),
            Statistics::Long(x) => x.num_values(),
            Statistics::Double(x) => x.num_values(),
            Statistics::Decimal64(x) => x.num_values(),
            Statistics::String(x) => x.num_values(),
            Statistics::Struct(x) => x.num_values(),
        }
    }

    fn num_present(&self) -> u64 {
        match self {
            Statistics::Boolean(x) => x.num_present(),
            Statistics::Long(x) => x.num_present(),
            Statistics::Double(x) => x.num_present(),
            Statistics::Decimal64(x) => x.num_present(),
            Statistics::String(x) => x.num_present(),
            Statistics::Struct(x) => x.num_present(),
        }
    }

    fn merge(&mut self, rhs: &Statistics) {
        match self {
            Statistics::Boolean(x) => x.merge(rhs.unwrap_boolean()),
            Statistics::Long(x) => x.merge(rhs.unwrap_long()),
            Statistics::Double(x) => x.merge(rhs.unwrap_double()),
            Statistics::Decimal64(x) => x.merge(rhs.unwrap_decimal64()),
            Statistics::String(x) => x.merge(rhs.unwrap_string()),
            Statistics::Struct(x) => x.merge(rhs.unwrap_struct()),
        }
    }
}
