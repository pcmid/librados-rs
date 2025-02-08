use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use librados_sys::{rados_getxattrs_end, rados_getxattrs_next, rados_xattrs_iter_t};

use crate::errors::{check_error, Error};

#[derive(Debug)]
pub struct Xattrs(HashMap<String, Vec<u8>>);

pub(crate) struct RadosXattrsIter {
    pub(crate) ptr: rados_xattrs_iter_t,
}

impl RadosXattrsIter {
    pub(crate) fn new(ptr: rados_xattrs_iter_t) -> Self {
        Self { ptr }
    }
}

impl Drop for RadosXattrsIter {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { rados_getxattrs_end(self.ptr) }
        }
    }
}

impl Xattrs {
    pub(crate) fn from_iter(value: &RadosXattrsIter) -> Result<Self, Error> {
        let mut xattrs = Xattrs(HashMap::new());
        if value.ptr.is_null() {
            return Ok(xattrs);
        }

        loop {
            let mut name_ptr: *const c_char = std::ptr::null_mut();
            let mut data_ptr: *const c_char = std::ptr::null_mut();
            let mut len = 0;

            let code =
                unsafe { rados_getxattrs_next(value.ptr, &mut name_ptr, &mut data_ptr, &mut len) };
            assert!(code <= 0);
            check_error(code)?;

            if len == 0 {
                break;
            }

            // TODO; should we free the name_ptr and data_ptr?
            let name = unsafe {
                let name = CStr::from_ptr(name_ptr).to_str()?.to_string();
                name
            };
            let data = unsafe {
                let data = slice::from_raw_parts(data_ptr as *const u8, len).to_vec();
                data
            };

            xattrs.0.insert(name, data);
        }

        Ok(xattrs)
    }

    pub fn iter(&self) -> Iter<String, Vec<u8>> {
        self.0.iter()
    }
}
