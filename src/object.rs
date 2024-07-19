use std::fmt::Debug;
use std::ops::DerefMut;
use std::os::raw::c_char;

use librados_sys::*;

use crate::buffer::MAX_BUF_SIZE;
use crate::errors::{check_error, Error, OVERFLOW_ERROR};
use crate::io::{AioCompletion, IoCtx};
use crate::rados::Rados;
use crate::xattr::{RadosXattrsIter, Xattrs};

#[derive(Debug)]
pub struct Object<'a> {
    name: String,
    pool_name: String,
    rados: &'a Rados,
}

impl<'a> Object<'a> {
    pub(crate) fn new(rados: &'a Rados, pool_name: &str, name: &str) -> Self {
        Object {
            name: name.to_string(),
            pool_name: pool_name.to_string(),
            rados,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn stat(&self) -> Result<Stat, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let comp = AioCompletion::new()?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut psize = Box::new(0u64);
        let mut pmtime = Box::new(timespec {
            tv_sec: 0,
            tv_nsec: 0,
        });

        let code = unsafe {
            rados_aio_stat2(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                psize.deref_mut() as *mut u64,
                pmtime.deref_mut() as *mut timespec,
            )
        };
        assert!(code <= 0);
        check_error(code)?;
        comp.await?;

        Ok(Stat {
            size: *psize,
            mtime: (pmtime.tv_sec as u64, pmtime.tv_nsec as u64),
        })
    }

    pub async fn read(&self, pos: u64, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.is_empty() {
            return Ok(0);
        }

        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let comp = AioCompletion::new()?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe {
            rados_aio_read(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                pos,
            )
        };
        assert!(code <= 0);
        check_error(code)?;

        comp.await
    }

    pub async fn write(&self, pos: u64, buf: &[u8]) -> Result<usize, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let comp = AioCompletion::new()?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe {
            rados_aio_write(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                buf.as_ptr() as *const c_char,
                buf.len(),
                pos,
            )
        };
        assert!(code <= 0);
        check_error(code)?;

        comp.await
    }

    pub async fn write_full(&self, data: &[u8]) -> Result<usize, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let comp = AioCompletion::new().unwrap();
        let key = std::ffi::CString::new(self.name()).unwrap();

        let code = unsafe {
            rados_aio_write_full(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                data.as_ptr() as *const c_char,
                data.len(),
            )
        };

        assert!(code <= 0);
        check_error(code)?;
        comp.await?;
        Ok(data.len())
    }

    pub async fn append(&self, data: &[u8]) -> Result<usize, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let comp = AioCompletion::new().unwrap();
        let key = std::ffi::CString::new(self.name()).unwrap();

        let code = unsafe {
            rados_aio_append(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                data.as_ptr() as *const c_char,
                data.len(),
            )
        };

        assert!(code <= 0);
        check_error(code)?;
        comp.await
    }

    pub async fn get_xattr(&self, name: &str) -> Result<Vec<u8>, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let name = std::ffi::CString::new(name).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut buf = Vec::with_capacity(64);

        loop {
            let comp = AioCompletion::new()?;
            let code = unsafe {
                rados_aio_getxattr(
                    io_ctx.ptr,
                    key.as_ptr() as *mut c_char,
                    comp.ptr,
                    name.as_ptr() as *const c_char,
                    buf.as_mut_ptr() as *mut c_char,
                    buf.capacity(),
                )
            };
            assert!(code <= 0);
            check_error(code)?;
            match comp.await {
                Ok(size) => {
                    unsafe { buf.set_len(size) };
                    break;
                }
                Err(e) => {
                    if e == *OVERFLOW_ERROR && buf.capacity() < MAX_BUF_SIZE {
                        buf.reserve(buf.capacity() * 2);
                    } else {
                        return Err(e.into());
                    }
                }
            }
        };

        Ok(buf)
    }

    pub async fn get_xattrs(&self) -> Result<Xattrs, Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let comp = AioCompletion::new()?;
        let mut iter = RadosXattrsIter::new(std::ptr::null_mut());

        let code = unsafe {
            rados_aio_getxattrs(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                &mut iter.ptr,
            )
        };
        assert!(code <= 0);
        check_error(code)?;
        comp.await?;

        let xattrs = Xattrs::from_iter(&iter)?;

        Ok(xattrs)
    }

    pub async fn set_xattr(&self, name: &str, value: &[u8]) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let name = std::ffi::CString::new(name).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let comp = AioCompletion::new()?;

        let code = unsafe {
            rados_aio_setxattr(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
                name.as_ptr() as *const c_char,
                value.as_ptr() as *const c_char,
                value.len(),
            )
        };
        assert!(code <= 0);
        check_error(code)?;
        match comp.await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn truncate(&self, size: u64) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.pool_name.clone())?;
        let key = std::ffi::CString::new(self.name()).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe {
            rados_trunc(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                size,
            )
        };

        assert!(code <= 0);
        check_error(code)
    }
}

#[derive(Debug)]
pub struct Stat {
    pub size: u64,
    pub mtime: (u64, u64),
}

#[cfg(test)]
mod tests {}

