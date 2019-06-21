use std::cmp::Ordering;
use std::ops::Add;
use std::borrow::{Borrow, ToOwned};

pub trait BaseStatistics {
    fn num_rows(&self) -> u64;
    fn has_null(&self) -> bool;
    fn merge(&mut self, rhs: &Self);
}
