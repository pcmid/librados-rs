use lazy_static::lazy_static;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};
use std::os::raw::c_int;
use std::string::ToString;

use librados_sys::*;

#[derive(Debug)]
pub enum Error {
    RadosError((isize, String)),
    NulError(std::ffi::NulError),
    IoError(std::io::Error),
    Utf8Error(std::str::Utf8Error),
    OtherError(anyhow::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::RadosError((code, message)) => {
                write!(f, "RadosError: code: {}, message: {}", code, message)
            }
            Error::NulError(error) => write!(f, "NulError: {}", error),
            Error::IoError(error) => write!(f, "IoError: {}", error),
            Error::Utf8Error(error) => write!(f, "Utf8Error: {}", error),
            Error::OtherError(message) => write!(f, "OtherError: {}", message),
        }
    }
}

pub(crate) fn check_error(code: c_int) -> Result<(), Error> {
    match code {
        num if num >= 0 => Ok(()),
        num => unsafe {
            CStr::from_ptr(strerror(-num))
                .to_str()
                .map_err(|e| e.into())
                .and_then(|s| Err(Error::RadosError((code as isize, s.to_string()))))
        },
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::RadosError(_) => Some(self),
            Error::NulError(e) => e.source(),
            Error::IoError(e) => e.source(),
            Error::Utf8Error(e) => e.source(),
            Error::OtherError(e) => e.source(),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(error: std::ffi::NulError) -> Self {
        Error::NulError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Error::Utf8Error(error)
    }
}

lazy_static! {
    pub(crate) static ref ERROR_RANGE: Error = check_error(-34).unwrap_err();
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::RadosError((code1, _)), Error::RadosError((code2, _))) => code1 == code2,
            _ => false,
        }
    }
}
