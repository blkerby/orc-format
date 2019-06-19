pub use boolean_rle::BooleanRLE;
pub use byte_rle::ByteRLE;
pub use int_rle_v1::{SignedIntRLEv1, UnsignedIntRLEv1};

mod boolean_rle;
mod byte_rle;
mod int_rle_v1;
mod varint;


#[cfg(test)]
mod tests {
    // use super::*;
    // use test_case_derive::test_case;

    // #[test_case(vec![], vec![] :: "empty")]
    // #[test_case(vec![10], vec![255, 10] :: "single")]
    // #[test_case(vec![10, 20, 30], vec![253, 10, 20, 30] :: "literal")]
    // #[test_case(vec![10, 10, 10], vec![0, 10] :: "run")]
    // #[test_case(vec![10, 20, 20, 20, 20], vec![255, 10, 1, 20] :: "literal then run")]
    // #[test_case(vec![10, 10, 10, 10, 10, 20, 30], vec![2, 10, 254, 20, 30] :: "run then literal")]
    // #[test_case(vec![10, 20, 20, 30], vec![252, 10, 20, 20, 30] :: "literal including false run")]
    // #[test_case(iter::repeat(10).take(131).collect(), vec![127, 10, 255, 10] :: "long run")]
    // #[test_case((0..140).collect(), [vec![128], (0..128).collect(), vec![244], (128..140).collect()].concat() :: "long literal")]
    // fn test_byte_rle(input: Vec<u8>, expected_output: Vec<u8>) {
    //     let mut rle = ByteRLE::new();
    //     for x in input {
    //         rle.write(x);
    //     }
    //     let mut out: Vec<u8> = Vec::new();
    //     rle.finish(&mut out).unwrap();
    //     assert_eq!(out, expected_output);
    // }

    // #[test_case(vec![], vec![] :: "empty")]
    // #[test_case(vec![true, false, true, false, true, false, false, false, true], vec![254, 0b10101000, 0b10000000] :: "literal")]
    // #[test_case(vec![false; 80], vec![7, 0] :: "run")]
    // fn test_boolean_rle(input: Vec<bool>, expected_output: Vec<u8>) {
    //     let mut rle = BooleanRLE::new();
    //     for x in input {
    //         rle.write(x);
    //     }
    //     let mut out: Vec<u8> = Vec::new();
    //     rle.finish(&mut out).unwrap();
    //     assert_eq!(out, expected_output);
    // }

    // fn test_write_varint_u64() {
    //     let cases: Vec<(u64, Vec<u8>)> = vec![
    //         (0, vec![0x00]),
    //         (1, vec![0x01]),
    //         (127, vec![0x7f]),
    //         (128, vec![0x80, 0x01]),
    //         (129, vec![0x81, 0x01]),
    //         (16383, vec![0xff, 0x7f]),
    //         (16384, vec![0x80, 0x80, 0x01]),
    //         (16385, vec![0x81, 0x80, 0x01]),
    //     ];
    //     for (input, expected_output) in cases {
    //         let mut buf = Buffer::new();
    //         input.write_varint(&mut buf);
    //         assert_eq!(buf.0, expected_output);
    //     }
    // }

    // #[test_case(0, vec![0])]
    // #[test_case(-1, vec![1])]
    // #[test_case(1, vec![2])]
    // #[test_case(-2, vec![3])]
    // #[test_case(2, vec![4])]
    // fn test_write_varint_i64(input: i64, expected_output: Vec<u8>) {
    //     let mut buf = Buffer::new();
    //     input.write_varint(&mut buf);
    //     assert_eq!(buf.0, expected_output);
    // }

    // #[test_case(vec![], vec![] :: "empty")]
    // #[test_case(vec![10], vec![255, 20] :: "single")]
    // #[test_case(vec![0, -1, 1, -2, 2], vec![251, 0, 1, 2, 3, 4] :: "literal")]
    // #[test_case(vec![10, 10, 10, 10], vec![1, 0, 20] :: "run_zero")]
    // #[test_case(vec![10, 15, 20, 25], vec![1, 5, 20] :: "run")]
    // #[test_case(vec![10, 15, 20, 25, 0], vec![1, 5, 20, 255, 0] :: "run then literal")]
    // fn test_signed_int_rle_v1(input: Vec<i64>, expected_output: Vec<u8>) {
    //     let mut rle = SignedIntRLEv1::new();
    //     for x in input {
    //         rle.write(x);
    //     }
    //     let mut out: Vec<u8> = Vec::new();
    //     rle.finish(&mut out).unwrap();
    //     assert_eq!(out, expected_output);
    // }

    // #[test_case(vec![7; 100], vec![97, 0, 7] :: "run")]
    // fn test_unsigned_int_rle_v1(input: Vec<u64>, expected_output: Vec<u8>) {
    //     let mut rle = UnsignedIntRLEv1::new();
    //     for x in input {
    //         rle.write(x);
    //     }
    //     let mut out: Vec<u8> = Vec::new();
    //     rle.finish(&mut out).unwrap();
    //     assert_eq!(out, expected_output);
    // }

}