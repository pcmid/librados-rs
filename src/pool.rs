use crate::errors::Error::OtherError;
use crate::errors::{check_error, Error, ERROR_RANGE};
use crate::io::{AioCompletion, IoCtx};
use crate::object::Object;
use crate::rados::Rados;
use crate::utils::c_char_ptr_to_string;
use anyhow::anyhow;
use librados_sys::*;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr::{null, null_mut};

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
        let key = std::ffi::CString::new(name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe { rados_aio_remove(io_ctx.ptr, key.as_ptr() as *mut c_char, comp.ptr) };
        assert!(code <= 0);
        check_error(code)?;
        comp.await?;

        Ok(())
    }

    pub fn list_objects(&self) -> Result<ListObjectResultIter, Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let mut list_ctx = null_mut();

        let code = unsafe { rados_nobjects_list_open(io_ctx.ptr, &mut list_ctx) };
        check_error(code)?;
        Ok(ListObjectResultIter {
            list_ctx,
            pool: &self,
        })
    }

    pub fn stat(&self) -> Result<Stat, Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let mut stat = rados_pool_stat_t {
            num_bytes: 0,
            num_kb: 0,
            num_objects: 0,
            num_object_clones: 0,
            num_object_copies: 0,
            num_objects_missing_on_primary: 0,
            num_objects_unfound: 0,
            num_objects_degraded: 0,
            num_rd: 0,
            num_rd_kb: 0,
            num_wr: 0,
            num_wr_kb: 0,
            num_user_bytes: 0,
            compressed_bytes_orig: 0,
            compressed_bytes: 0,
            compressed_bytes_alloc: 0,
        };
        let code = unsafe { rados_ioctx_pool_stat(io_ctx.ptr, &mut stat) };
        check_error(code)?;

        Ok(Stat {
            num_bytes: stat.num_bytes,
            num_kb: stat.num_kb,
            num_objects: stat.num_objects,
            num_object_clones: stat.num_object_clones,
            num_object_copies: stat.num_object_copies,
            num_objects_missing_on_primary: stat.num_objects_missing_on_primary,
            num_objects_unfound: stat.num_objects_unfound,
            num_objects_degraded: stat.num_objects_degraded,
            num_rd: stat.num_rd,
            num_rd_kb: stat.num_rd_kb,
            num_wr: stat.num_wr,
            num_wr_kb: stat.num_wr_kb,
            num_user_bytes: stat.num_user_bytes,
            compressed_bytes_orig: stat.compressed_bytes_orig,
            compressed_bytes: stat.compressed_bytes,
            compressed_bytes_alloc: stat.compressed_bytes_alloc,
        })
    }

    pub fn snapshot_create(&self, snap_name: &str) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let snap_name = std::ffi::CString::new(snap_name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code =
            unsafe { rados_ioctx_snap_create(io_ctx.ptr, snap_name.as_ptr() as *const c_char) };
        assert!(code <= 0);
        check_error(code)
    }

    pub fn snapshot_remove(&self, snap_name: &str) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let snap_name = std::ffi::CString::new(snap_name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code =
            unsafe { rados_ioctx_snap_remove(io_ctx.ptr, snap_name.as_ptr() as *const c_char) };
        assert!(code <= 0);
        check_error(code)
    }

    pub fn snapshot_rollback_for(&self, object: &Object, snap_name: &str) -> Result<(), Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let object_name = std::ffi::CString::new(object.name())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let snap_name = std::ffi::CString::new(snap_name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let code = unsafe {
            rados_ioctx_snap_rollback(
                io_ctx.ptr,
                object_name.as_ptr(),
                snap_name.as_ptr() as *const c_char,
            )
        };
        assert!(code <= 0);
        check_error(code)
    }

    pub fn snapshot_list(&self) -> Result<Vec<u64>, Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let mut snap_ids: Vec<u64> = Vec::with_capacity(16);
        let max_len: usize = (1 << 31) - 1;

        while snap_ids.capacity() <= max_len {
            let code = unsafe {
                rados_ioctx_snap_list(
                    io_ctx.ptr,
                    snap_ids.as_mut_ptr(),
                    snap_ids.capacity() as i32,
                )
            };
            if code >= 0 {
                unsafe {
                    snap_ids.set_len(code as usize);
                }
                return Ok(snap_ids);
            }
            match { check_error(code) } {
                Err(e) => {
                    if e == *ERROR_RANGE {
                        snap_ids.reserve(snap_ids.capacity() * 2);
                    } else {
                        return Err(e);
                    }
                }
                Ok(_) => {
                    return Err(OtherError(anyhow!("unexpected success")));
                }
            }
        }
        Err(OtherError(anyhow!("too many snapshots")))
    }

    pub fn snapshot_lookup(&self, snap_name: &str) -> Result<u64, Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;
        let snap_name = CString::new(snap_name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut snap_id: u64 = 0;

        let code = unsafe {
            rados_ioctx_snap_lookup(
                io_ctx.ptr,
                snap_name.as_ptr() as *const c_char,
                &mut snap_id as *mut u64,
            )
        };
        assert!(code <= 0);
        check_error(code)?;
        Ok(snap_id)
    }

    pub fn snapshot_get_name(&self, snap_id: u64) -> Result<String, Error> {
        let io_ctx = IoCtx::new(self.rados, self.name.clone())?;

        let max_len = 256;
        let mut snap_name = Vec::with_capacity(max_len);

        let code = unsafe {
            rados_ioctx_snap_get_name(
                io_ctx.ptr,
                snap_id,
                snap_name.as_mut_ptr() as *mut c_char,
                snap_name.capacity() as i32,
            )
        };
        check_error(code).map(|_| {
            unsafe {
                snap_name.set_len(snap_name.capacity());
            }

            if let Some(pos) = snap_name.iter().position(|&x| x == 0) {
                String::from_utf8_lossy(&snap_name[..pos]).to_string()
            } else {
                String::from_utf8_lossy(&snap_name).to_string()
            }
        })
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Stat {
    pub num_bytes: u64,
    pub num_kb: u64,
    pub num_objects: u64,
    pub num_object_clones: u64,
    pub num_object_copies: u64,
    pub num_objects_missing_on_primary: u64,
    pub num_objects_unfound: u64,
    pub num_objects_degraded: u64,
    pub num_rd: u64,
    pub num_rd_kb: u64,
    pub num_wr: u64,
    pub num_wr_kb: u64,
    pub num_user_bytes: u64,
    pub compressed_bytes_orig: u64,
    pub compressed_bytes: u64,
    pub compressed_bytes_alloc: u64,
}

pub struct ListObjectResultIter<'a> {
    list_ctx: rados_list_ctx_t,

    pool: &'a Pool<'a>,
}

impl<'a> Iterator for ListObjectResultIter<'a> {
    type Item = Object<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut entry = null();
        let mut key = null();
        let mut nspace = null();
        let mut entry_size: usize = 0;
        let mut key_size: usize = 0;
        let mut nspace_size: usize = 0;

        let code = unsafe {
            rados_nobjects_list_next2(
                self.list_ctx,
                &mut entry,
                &mut key,
                &mut nspace,
                &mut entry_size,
                &mut key_size,
                &mut nspace_size,
            )
        };
        check_error(code).map_or(None, |_| unsafe {
            let entry = c_char_ptr_to_string(entry, entry_size);
            #[allow(unused_variables)]
            let key = c_char_ptr_to_string(key, key_size);
            #[allow(unused_variables)]
            let nspace = c_char_ptr_to_string(nspace, nspace_size);
            Some(Object::new(self.pool.rados, &self.pool.name, &entry))
        })
    }
}

impl Drop for ListObjectResultIter<'_> {
    fn drop(&mut self) {
        unsafe {
            rados_nobjects_list_close(self.list_ctx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Error::RadosError;
    use crate::rados::Rados;

    #[tokio::test]
    async fn test_pool() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        let stats = pool
            .list_objects()
            .unwrap()
            .map(async move |obj| {
                let stat = obj.stat().await.unwrap();
                println!("{}: {:?}", obj.name(), stat);
            })
            .collect::<Vec<_>>();

        futures::future::join_all(stats).await;
    }

    #[tokio::test]
    async fn test_pool_stat() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        let stat = pool.stat().unwrap();
        println!("{:?}", stat);
    }

    #[tokio::test]
    async fn test_pool_snapshot() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        let snaps = pool.snapshot_list().unwrap();
        println!("{:?}", snaps);
    }

    #[tokio::test]
    async fn test_pool_snapshot_lookup() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        let snap_id = pool.snapshot_lookup("snap1").unwrap();
        println!("{:?}", snap_id);
    }

    #[tokio::test]
    async fn test_pool_snapshot_get_name() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        let snap_name = pool.snapshot_get_name(1).unwrap();
        println!("{:?}", snap_name);
    }

    #[tokio::test]
    async fn test_pool_snapshot_create_remove() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        match pool.snapshot_lookup("snap1") {
            Ok(_) => {
                pool.snapshot_remove("snap1").unwrap();
            }
            Err(_) => {}
        }

        pool.snapshot_create("snap1").unwrap();
        pool.snapshot_remove("snap1").unwrap();

        assert_eq!(
            pool.snapshot_lookup("snap1").unwrap_err(),
            RadosError((-2, "No such file or directory".to_string()))
        );
    }

    #[tokio::test]
    async fn test_pool_snapshot_rollback() {
        let rados = Rados::new("test/ceph.conf", "ceph", "client.admin").unwrap();
        let pool = Pool::new(&rados, "test");

        match pool.snapshot_lookup("snap1") {
            Ok(_) => {
                pool.snapshot_remove("snap1").unwrap();
            }
            Err(_) => {}
        }

        let obj = pool.put_object("obj1", b"test1").await.unwrap();
        pool.snapshot_create("snap1").unwrap();
        obj.write_full(b"test2").await.unwrap();

        let mut buffer = [0; 8];
        let len = obj.read(0, &mut buffer).await.unwrap();
        assert_eq!(len, 5);
        assert_eq!(&buffer[..len], b"test2");

        pool.snapshot_rollback_for(&obj, "snap1").unwrap();

        let len = obj.read(0, &mut buffer).await.unwrap();
        assert_eq!(len, 5);
        assert_eq!(&buffer[..len], b"test1");

        pool.snapshot_remove("snap1").unwrap();
    }
}
