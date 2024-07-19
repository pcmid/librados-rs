#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rados_version() {
        let mut major: std::os::raw::c_int = 0;
        let mut minor: std::os::raw::c_int = 0;
        let mut extra: std::os::raw::c_int = 0;

        unsafe {
            rados_version(&mut major, &mut minor, &mut extra);
        };
        assert_eq!(major, 3);
        assert_eq!(minor, 0);
        assert_eq!(extra, 0);
    }
}