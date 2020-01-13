use std::io::{Write, Result};
use std::fs::{File};
use std::path::{Path, PathBuf};
use std::fs::rename;
use std::io;
use chrono::{DateTime, Utc, Duration};
use std::error::Error;
use derive_more::Display;

pub struct RotatingFile<L> {
    inner: File,
    path: Box<PathBuf>,
    rotation_policy: L,
    naming_policy: fn(&PathBuf) -> Option<PathBuf>
}

#[derive(Debug, Display)]
#[display(fmt = "Rotating file error")]
pub struct RotatingFileError;

impl Error for RotatingFileError {

}

impl<L> RotatingFile<L>
    where L: RotationPolicy
{
    pub fn new(path: Box<PathBuf>, rotation_policy: L, new_name: fn(&PathBuf) -> Option<PathBuf>) -> Result<Self> {
        Ok(RotatingFile {
            inner: File::create(path.as_ref())?,
            path,
            rotation_policy,
            naming_policy: new_name
        })
    }

    fn rotate(&mut self) -> Result<()> {
        self.inner.flush()?;
        let &mut RotatingFile { ref path, naming_policy, ..} = self;
        // TODO: use a rwlock
        let new_path = (naming_policy)(path.as_ref()).ok_or(std::io::Error::new(std::io::ErrorKind::Other, RotatingFileError))?;
        self.rotation_policy.set_last_flush(Utc::now());
        self.path = Box::new(new_path.clone());
        self.inner = File::create(new_path)?;
        Ok(())
    }
}

impl<L>  Write for RotatingFile<L>
    where L: RotationPolicy {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.rotation_policy.should_rotate(&self.path, &self.inner)? {
            self.rotate()?;
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

pub trait RotationPolicy {
    fn set_last_flush(&mut self, d: DateTime<Utc>);

    fn should_rotate(&self, p: &Path, f: &File) -> io::Result<bool>;
}

#[derive(Clone)]
pub struct SizeAndExpirationPolicy {
    pub max_size_b: u64,
    pub max_time_ms: Duration,
    pub last_flush: DateTime<Utc>,
}


impl RotationPolicy for SizeAndExpirationPolicy {
    fn set_last_flush(&mut self, d: DateTime<Utc>) {
        self.last_flush = d;
    }

    fn should_rotate(&self, _path: &Path, file: &File) -> io::Result<bool> {
        let metadata = file.metadata()?;
        let now = Utc::now();
        let elapsed = now - self.last_flush;
        Ok(self.max_size_b < metadata.len() || self.max_time_ms < elapsed)
    }
}
