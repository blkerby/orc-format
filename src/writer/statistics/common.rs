use std::cmp::Ordering;
use std::ops::Add;
use std::borrow::{Borrow, ToOwned};

pub trait BaseStatistics {
    fn num_values(&self) -> u64;
    fn num_present(&self) -> u64;
    fn merge(&mut self, rhs: &Self);

    fn has_null(&self) -> bool { 
        self.num_values() != self.num_present()
    }
}
