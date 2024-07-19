use std::ffi::CString;
use std::ptr::{self};

use librados_sys::*;

use crate::errors::{self, check_error};

#[derive(Debug)]
pub(crate) struct Rados {
    pub(crate) ptr: rados_t,
}

impl Rados {
    pub(crate) fn new(conf_file: &str, cluster_name: &str, user_name: &str) -> Result<Self, errors::Error> {
        let mut ptr: rados_t = ptr::null_mut();
        let cluster_name = CString::new(cluster_name)?;
        let user_name = CString::new(user_name)?;

        let ptr = unsafe {
            let code = rados_create2(
                &mut ptr,
                cluster_name.as_ptr(),
                user_name.as_ptr(),
                0,
            );
            check_error(code)?;
            ptr
        };

        if !conf_file.is_empty() {
            let conf_file = CString::new(conf_file)?;
            unsafe {
                let code = rados_conf_read_file(
                    ptr,
                    conf_file.as_ptr(),
                );
                check_error(code).map_err(
                    |e| {
                        rados_shutdown(ptr);
                        e
                    }
                )?;
            };
        }

        let code = unsafe { rados_connect(ptr) };
        check_error(code)?;

        Ok(Rados {
            ptr,
        })
    }
}

impl Drop for Rados {
    fn drop(&mut self) {
        unsafe {
            rados_shutdown(self.ptr);
        }
    }
}

