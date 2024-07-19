use std::os::raw::c_char;

use librados_sys::{rados_aio_remove, rados_list_ctx_t, rados_object_list_cursor, rados_object_list_cursor_free};

use crate::errors::{check_error, Error};
use crate::io::{AioCompletion, IoCtx};
use crate::object::Object;
use crate::rados::Rados;

pub struct Pool<'a> {
    name: String,
    rados: &'a Rados,
}

impl<'a> Pool<'a> {
    pub(crate) fn new(rados: &'a Rados, name: &str) -> Self {
        Pool {
            name: name.to_string(),
            rados,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn get_object(&self, name: &str) -> Result<Object, Error> {
        let obj = Object::new(self.rados, &self.name, name);
        obj.stat().await?;
        Ok(obj)
    }

    pub async fn put_object(&self, name: &str, data: &[u8]) -> Result<Object, Error> {
        let obj = Object::new(self.rados, &self.name, name);
        obj.write_full(data).await?;
        Ok(obj)
    }

    pub async fn create_object(&self, name: &str) -> Result<Object, Error> {
        self.put_object(name, &[]).await
    }

    pub async fn remove_object(&self, name: &str) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let comp = AioCompletion::new()?;
        let key = std::ffi::CString::new(name).
            map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe {
            rados_aio_remove(
                io_ctx.ptr,
                key.as_ptr() as *mut c_char,
                comp.ptr,
            )
        };
        assert!(code <= 0);
        check_error(code)?;
        comp.await?;

        Ok(())
    }

    pub async fn list_objects(&self) -> Result<ListObjectResult, Error> {
        unimplemented!("list_objects")
    }
}

#[allow(dead_code)]
pub struct ListObjectResult<'a> {
    io_ctx: IoCtx<'a>,
    list_ctx: rados_list_ctx_t,
}

#[allow(dead_code)]
impl<'a> ListObjectResult<'a> {
    fn new(io_ctx: IoCtx<'a>, list_ctx: rados_list_ctx_t) -> Self {
        ListObjectResult {
            io_ctx,
            list_ctx,
        }
    }

    pub fn iter(&mut self) -> Result<ListObjectResultIter, Error> {
        unimplemented!("iter")
    }
}

#[allow(dead_code)]
pub struct ListObjectResultIter<'a> {
    io_ctx: &'a IoCtx<'a>,
    list_ctx: &'a rados_list_ctx_t,
    cursor: rados_object_list_cursor,
}

impl Iterator for ListObjectResultIter<'_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!("next")
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

impl Drop for ListObjectResultIter<'_> {
    fn drop(&mut self) {
        unsafe {
            rados_object_list_cursor_free(self.io_ctx.ptr, self.cursor);
        }
    }
}

mod tests {}