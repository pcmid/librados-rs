use std::os::raw::c_char;
use std::slice;
use std::str;

pub(crate) unsafe fn c_char_ptr_to_string(ptr: *const c_char, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }

    let slice = slice::from_raw_parts(ptr as *const u8, len);
    match str::from_utf8(slice) {
        Ok(s) => s.to_string(),
        Err(_) => String::new(),
    }
}
