use std::ffi::{c_char, c_int, CString};
use std::io::{BufRead, Cursor};
use std::sync::Arc;

use librados_sys::{rados_pool_create, rados_pool_delete, rados_pool_list, rados_pool_lookup};

use crate::errors::{check_error, Error};
use crate::pool::Pool;
use crate::rados::Rados;

#[derive(Debug)]
pub struct Cluster {
    pub(crate) rados: Arc<Rados>,
}

impl Cluster {
    pub fn new(conf_file: &str, cluster_name: &str, user_name: &str) -> Result<Self, Error> {
        let rados: Rados = Rados::new(conf_file, cluster_name, user_name)?;
        Ok(Cluster {
            rados: Arc::new(rados),
        })
    }

    pub fn pool_lookup(&self, pool_name: &str) -> Result<Pool, Error> {
        let name = CString::new(pool_name)?;
        let id = unsafe { rados_pool_lookup(self.rados.ptr, name.as_ptr()) };
        if id < 0 {
            check_error(id as c_int)?;
        }

        Ok(Pool::new(&self.rados, pool_name))
    }

    pub fn pool_create(&self, pool_name: &str) -> Result<Pool, Error> {
        let pool_name = CString::new(pool_name)?;
        unsafe {
            let code = rados_pool_create(self.rados.ptr, pool_name.as_ptr());
            check_error(code)?;
        }

        self.pool_lookup(pool_name.to_str()?)
    }

    pub fn pool_delete(&self, pool_name: &str) -> Result<(), Error> {
        let pool_name = CString::new(pool_name)?;
        let code = unsafe { rados_pool_delete(self.rados.ptr, pool_name.as_ptr()) };
        check_error(code)
    }

    pub fn pool_list(&self) -> Result<Vec<Pool>, Error> {
        let mut pools: Vec<Pool> = Vec::new();
        let mut pool_buffer: Vec<u8> = Vec::with_capacity(500);
        unsafe {
            let rados = self.rados.clone();

            let len = rados_pool_list(
                rados.ptr,
                pool_buffer.as_mut_ptr() as *mut c_char,
                pool_buffer.capacity(),
            );

            if len < 0 {
                check_error(len)?;
            }


            if len > pool_buffer.capacity() as i32 {
                pool_buffer.reserve(len as usize);
                let len = rados_pool_list(
                    rados.ptr,
                    pool_buffer.as_mut_ptr() as *mut c_char,
                    pool_buffer.capacity(),
                );

                if len < 0 {
                    check_error(len as i32)?;
                }

                pool_buffer.set_len(len as usize);
            } else {
                pool_buffer.set_len(len as usize);
            }
        }

        let mut cursor = Cursor::new(&pool_buffer);
        loop {
            let mut string_buf: Vec<u8> = Vec::new();
            let read = cursor.read_until(0x00, &mut string_buf)?;
            if read == 0 || read == 1 {
                break;
            } else {
                pools.push(Pool::new(
                    &self.rados,
                    &String::from_utf8_lossy(&string_buf[..read - 1]),
                ));
            }
        }

        Ok(pools)
    }
}

#[cfg(test)]
mod tests {}
