use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    Gz,
    Z,
    Deflate,
    None,
}

#[derive(Clone, Debug, Copy, Serialize, Deserialize)]
pub struct Compression {
    pub algorithm: CompressionType,
    pub level: Option<u32>,
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            algorithm: CompressionType::None,
            level: None,
        }
    }
}

impl Compression {
    pub fn none() -> Self {
        Self {
            algorithm: CompressionType::None,
            level: None,
        }
    }

    pub fn wrap_writer<W: 'static + Write + Send>(&self, w: W) -> Box<dyn Write + Send> {
        let level = self.level.unwrap_or(5);
        match self.algorithm {
            CompressionType::Gz => Box::new(flate2::write::GzEncoder::new(w, flate2::Compression::new(level))),
            CompressionType::Z => Box::new(flate2::write::ZlibEncoder::new(w, flate2::Compression::new(level))),
            CompressionType::Deflate => {
                Box::new(flate2::write::DeflateEncoder::new(w, flate2::Compression::new(level)))
            }
            CompressionType::None => Box::new(w),
        }
    }

    pub fn wrap_reader<R: 'static + Read + Send>(&self, r: R) -> Box<dyn Read + Send> {
        match self.algorithm {
            CompressionType::Gz => Box::new(flate2::read::GzDecoder::new(r)),
            CompressionType::Z => Box::new(flate2::read::ZlibDecoder::new(r)),
            CompressionType::Deflate => Box::new(flate2::read::DeflateDecoder::new(r)),
            CompressionType::None => Box::new(r),
        }
    }

    pub fn wrap_ext<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        let current_path = path.as_ref();
        let ext = match self.algorithm {
            CompressionType::Gz => "gz",
            CompressionType::Z => "z",
            CompressionType::Deflate => "df",
            CompressionType::None => "",
        };
        let current_ext = current_path
            .extension()
            .and_then(|os_str| os_str.to_str())
            .unwrap_or("");
        let ext = if current_ext.is_empty() {
            ext.to_string()
        } else {
            format!("{}.{}", current_ext, ext)
        };
        current_path.with_extension(ext)
    }
}
