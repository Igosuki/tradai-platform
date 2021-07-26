use std::fs::File;
use std::io::{Result, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use derive_more::Display;
use log::Level::*;
use std::error::Error;
use std::io;

pub struct RotatingFile<L> {
    inner: File,
    path: Box<PathBuf>,
    rotation_policy: L,
    naming_policy: fn(&Path) -> Option<PathBuf>,
    on_new_header: Option<Vec<u8>>,
}

#[derive(Debug, Display)]
#[display(fmt = "Rotating file error")]
pub struct RotatingFileError;

impl Error for RotatingFileError {}

impl<L> RotatingFile<L>
where
    L: RotationPolicy,
{
    pub fn new(
        path: Box<PathBuf>,
        rotation_policy: L,
        new_name: fn(&Path) -> Option<PathBuf>,
        on_new_header: Option<Vec<u8>>,
    ) -> Result<Self> {
        Ok(RotatingFile {
            inner: File::create(path.as_ref())?,
            path,
            rotation_policy,
            naming_policy: new_name,
            on_new_header,
        })
    }

    fn rotate(&mut self) -> Result<()> {
        if log_enabled!(Trace) {
            trace!("Flushing {:?}", &self.path);
        }
        self.inner.flush()?;
        let &mut RotatingFile {
            ref path,
            naming_policy,
            ..
        } = self;
        let new_path = (naming_policy)(path.as_ref())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, RotatingFileError))?;
        self.rotation_policy.set_last_flush(Utc::now());
        self.path = Box::new(new_path.clone());
        let f = File::create(new_path)?;
        if let Some(h) = self.on_new_header.clone() {
            let mut fd = f.try_clone()?;
            fd.write_all(h.as_ref())?;
            fd.flush()?;
            drop(fd)
        }
        self.inner = f;
        Ok(())
    }
}

impl<L> Write for RotatingFile<L>
where
    L: RotationPolicy,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.rotation_policy.should_rotate(&self.path, &self.inner)? {
            self.rotate()?;
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> { self.inner.flush() }
}

pub trait RotationPolicy {
    fn set_last_flush(&mut self, d: DateTime<Utc>);

    fn should_rotate(&mut self, p: &Path, f: &File) -> io::Result<bool>;
}

#[derive(Clone)]
pub struct SizeAndExpirationPolicy {
    pub max_size_b: u64,
    pub max_time_ms: Duration,
    pub last_flush: Option<DateTime<Utc>>,
}

impl RotationPolicy for SizeAndExpirationPolicy {
    fn set_last_flush(&mut self, d: DateTime<Utc>) { self.last_flush = Some(d); }

    fn should_rotate(&mut self, _path: &Path, file: &File) -> io::Result<bool> {
        match self.last_flush {
            // If last flush happened, check if the rotation policy applies
            Some(dt) => {
                let metadata = file.metadata()?;
                let now = Utc::now();
                let elapsed = now - dt;
                Ok(self.max_size_b < metadata.len() || self.max_time_ms < elapsed)
            }
            // If never flushed, start counting from now
            None => {
                self.set_last_flush(Utc::now());
                Ok(false)
            }
        }
    }
}
