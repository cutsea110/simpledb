use anyhow::Result;
use core::fmt;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use super::{block_id::BlockId, page::Page};

#[derive(Debug)]
enum FileMgrError {
    ParseFailed,
    FileAccessFailed(String),
}

impl std::error::Error for FileMgrError {}
impl fmt::Display for FileMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileMgrError::ParseFailed => write!(f, "parse failed"),
            FileMgrError::FileAccessFailed(filename) => {
                write!(f, "file accecss failed: {}", filename)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileMgr {
    db_directory: String,
    blocksize: i32,
    is_new: bool,
    open_files: HashMap<String, Arc<Mutex<File>>>,
}

impl FileMgr {
    pub fn new(db_directory: &str, blocksize: i32) -> Result<Self> {
        let path = Path::new(db_directory);
        let is_new = !path.exists();

        if is_new {
            fs::create_dir_all(path)?;
        }

        for entry in fs::read_dir(path)? {
            let entry_path = entry?.path();
            let filename = match entry_path.as_path().to_str() {
                Some(s) => s.to_string(),
                None => return Err(From::from(FileMgrError::ParseFailed)),
            };

            if filename.starts_with("temp") {
                fs::remove_file(entry_path)?;
            }
        }

        Ok(Self {
            db_directory: db_directory.to_string(),
            blocksize,
            is_new,
            open_files: HashMap::new(),
        })
    }
    // synchronized
    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> Result<()> {
        let offset = blk.number() * self.blocksize;

        if let Some(file) = self.get_file(blk.file_name().as_str()) {
            let mut f = file.lock().unwrap();
            f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;

            let read_len = f.read(p.contents())?;
            let p_len = p.contents().len();
            if read_len < p_len {
                let tmp = vec![0; p_len - read_len];
                f.write_all(&tmp)?;

                for i in read_len..p_len {
                    p.contents()[i] = 0;
                }
            }

            return Ok(());
        }

        Err(From::from(FileMgrError::FileAccessFailed(blk.file_name())))
    }
    // synchronized
    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> Result<()> {
        let offset = blk.number() * self.blocksize;

        if let Some(file) = self.get_file(blk.file_name().as_str()) {
            let mut f = file.lock().unwrap();
            f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
            f.write_all(p.contents())?;

            return Ok(());
        }

        Err(From::from(FileMgrError::FileAccessFailed(blk.file_name())))
    }
    // synchronized
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let new_blknum = self.length(filename)?;
        let blk = BlockId::new(filename, new_blknum);
        let b: Vec<u8> = vec![0u8; self.blocksize as usize];
        let offset = blk.number() * self.blocksize;

        if let Some(file) = self.get_file(blk.file_name().as_str()) {
            let mut f = file.lock().unwrap();
            f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
            f.write_all(&b)?;

            return Ok(blk);
        }

        Err(From::from(FileMgrError::FileAccessFailed(
            filename.to_string(),
        )))
    }
    pub fn length(&mut self, filename: &str) -> Result<i32> {
        let path = Path::new(&self.db_directory).join(filename);
        let _ = self.get_file(filename).unwrap();
        let meta = fs::metadata(&path)?;

        // ceiling
        Ok((meta.len() as i32 + self.blocksize - 1) / self.blocksize)
    }
    pub fn is_new(&self) -> bool {
        self.is_new
    }
    pub fn block_size(&self) -> i32 {
        self.blocksize
    }
    fn get_file(&mut self, filename: &str) -> Option<&mut Arc<Mutex<File>>> {
        let path = Path::new(&self.db_directory).join(filename);

        let f = self
            .open_files
            .entry(filename.to_string())
            .or_insert(Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path)
                    .unwrap(),
            )));

        Some(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    #[test]
    fn unit_test() {
        if Path::new("_filetest").exists() {
            fs::remove_dir_all("_filetest").expect("cleanup");
        }

        let mut fm = FileMgr::new("_filetest", 400).expect("create FileMgr");
        let blk = BlockId::new("testfile", 2);
        let mut p1 = Page::new_from_size(fm.block_size() as usize);
        let pos1 = 0; // 88;
        p1.set_string(pos1, "abcdefghijklm".to_string())
            .expect("set string");
        let size = Page::max_length("abcdefghijjklm".len());
        let pos2 = pos1 + size;
        p1.set_i32(pos2, 345).expect("set i32");
        fm.write(&blk, &mut p1).expect("write p1 to blk");

        let mut p2 = Page::new_from_size(fm.block_size() as usize);
        fm.read(&blk, &mut p2).expect("read blk to p2");

        assert_eq!(345, p2.get_i32(pos2).expect("get i32"));
        assert_eq!(
            "abcdefghijklm".to_string(),
            p2.get_string(pos1).expect("get string")
        );
    }
}
