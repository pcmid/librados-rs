use std::future::Future;
use std::marker::PhantomData;
use std::mem::transmute;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::task::{Context, Poll, Waker};

use librados_sys::*;

use crate::errors::{check_error, Error};
use crate::rados::Rados;

#[derive(Debug)]
pub(crate) struct IoCtx<'a> {
    pub(crate) ptr: rados_ioctx_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a> IoCtx<'a> {
    pub(crate) fn new(rados: &'a Rados, pool_name: String) -> Result<Self, Error> {
        let mut ptr = std::ptr::null_mut();
        let pool_name = std::ffi::CString::new(pool_name)?;

        let code = unsafe {
            rados_ioctx_create(rados.ptr, pool_name.as_ptr(), &mut ptr)
        };
        check_error(code)?;

        Ok(IoCtx {
            ptr,
            _marker: PhantomData,
        })
    }
}

impl Drop for IoCtx<'_> {
    fn drop(&mut self) {
        unsafe {
            rados_ioctx_destroy(self.ptr);
        }
    }
}

#[derive(Debug)]
pub(crate) struct AioCompletion {
    pub(crate) ptr: rados_completion_t,
    waker: Arc<Box<AtomicPtr<Waker>>>,
}

impl AioCompletion {
    pub(crate) fn new() -> Result<Self, Error> {
        let mut comp = AioCompletion {
            ptr: std::ptr::null_mut(),
            waker: Arc::new(Box::new(AtomicPtr::new(std::ptr::null_mut()))),
        };

        let cb: rados_callback_t = Some(aio_callback);

        let code = unsafe {
            let waker = transmute(comp.waker.clone());
            rados_aio_create_completion2(waker, cb, &mut comp.ptr)
        };
        check_error(code)?;
        Ok(comp)
    }

    pub(crate) fn set_waker(&mut self, waker: Waker) {
        let waker = Box::new(waker);
        let waker = Box::into_raw(waker);
        let old = self.waker.swap(waker, Ordering::SeqCst);
        if !old.is_null() {
            unsafe {
                let _waker = Box::from_raw(old);
                _waker.wake_by_ref();
            }
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        unsafe {
            rados_aio_is_complete(self.ptr) != 0
        }
    }

    pub(crate) fn get_return_value(&self) -> Result<usize, Error> {
        let ret = unsafe {
            rados_aio_get_return_value(self.ptr)
        };

        if ret < 0 {
            check_error(ret)?;
        }
        Ok(ret as usize)
    }

    #[allow(dead_code)]
    pub(crate) fn wait_for_complete(&self) -> Result<usize, Error> {
        let ret = unsafe {
            rados_aio_get_return_value(self.ptr)
        };
        if ret < 0 {
            check_error(ret)?;
        }
        Ok(ret as usize)
    }

    #[allow(dead_code)]
    pub(crate) fn is_complete_and_cb(&self) -> bool {
        unsafe {
            rados_aio_is_complete_and_cb(self.ptr) != 0
        }
    }

    #[allow(dead_code)]
    pub(crate) fn wait_for_complete_and_cb(&self) -> Result<usize, Error> {
        let ret = unsafe {
            // the code is always 0
            let code = rados_aio_wait_for_complete_and_cb(self.ptr);
            assert_eq!(code, 0);
            rados_aio_get_return_value(self.ptr)
        };

        if ret < 0 {
            check_error(ret)?;
        }
        Ok(ret as usize)
    }
}

impl Drop for AioCompletion {
    fn drop(&mut self) {
        unsafe {
            rados_aio_release(self.ptr);
            let waker = self.waker.swap(std::ptr::null_mut(), Ordering::SeqCst);
            if !waker.is_null() {
                let _waker = Box::from_raw(waker);
                // waker.wake_by_ref();
            }
        }
    }
}

impl Future for AioCompletion {
    type Output = Result<usize, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_complete() {
            Poll::Ready(self.get_return_value())
        } else {
            self.set_waker(cx.waker().clone());
            Poll::Pending
        }
    }
}


unsafe extern "C" fn aio_callback(_completion: rados_completion_t, _arg: *mut std::os::raw::c_void) {
    let waker: Arc<Box<AtomicPtr<Waker>>> = transmute(_arg);
    let waker = waker.swap(std::ptr::null_mut(), Ordering::SeqCst);
    if !waker.is_null() {
        Box::from_raw(waker).wake_by_ref()
    }
}

mod tests {}
