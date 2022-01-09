use std::io::{Read, Write};

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
        let level = self.level.unwrap_or(5);
        match self.algorithm {
            CompressionType::Gz => Box::new(flate2::read::GzDecoder::new(r)),
            CompressionType::Z => Box::new(flate2::read::ZlibDecoder::new(r)),
            CompressionType::Deflate => Box::new(flate2::read::DeflateEncoder::new(r, flate2::Compression::new(level))),
            CompressionType::None => Box::new(r),
        }
    }
}
