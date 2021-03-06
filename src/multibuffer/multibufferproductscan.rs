use anyhow::Result;
use chrono::NaiveDate;
use core::fmt;
use std::sync::{Arc, Mutex};

use super::{bufferneeds, chunkscan::ChunkScan};
use crate::{
    materialize::sortscan::SortScan,
    query::{constant::Constant, productscan::ProductScan, scan::Scan, updatescan::UpdateScan},
    record::{layout::Layout, tablescan::TableScan},
    tx::transaction::Transaction,
};

#[derive(Debug)]
pub enum MultibufferProductScanError {
    NoRhsScan,
    NoProductScan,
    DowncastError,
}

impl std::error::Error for MultibufferProductScanError {}
impl fmt::Display for MultibufferProductScanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MultibufferProductScanError::NoRhsScan => {
                write!(f, "no rhs scan")
            }
            MultibufferProductScanError::NoProductScan => {
                write!(f, "no product scan")
            }
            MultibufferProductScanError::DowncastError => {
                write!(f, "downcast error")
            }
        }
    }
}

#[derive(Clone)]
pub struct MultibufferProductScan {
    tx: Arc<Mutex<Transaction>>,
    lhsscan: Arc<Mutex<dyn Scan>>,
    rhsscan: Option<Arc<Mutex<dyn Scan>>>,
    prodscan: Option<Arc<Mutex<dyn Scan>>>,
    filename: String,
    layout: Arc<Layout>,
    chunksize: i32,
    nextblknum: i32,
    filesize: i32,
}

impl MultibufferProductScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhsscan: Arc<Mutex<dyn Scan>>,
        tblname: &str,
        layout: Arc<Layout>,
    ) -> Self {
        let filename = format!("{}.tbl", tblname);
        let filesize = tx.lock().unwrap().size(&filename).unwrap();
        let available = tx.lock().unwrap().available_buffs() as i32;
        let chunksize = bufferneeds::best_factor(available, filesize);

        let mut scan = Self {
            tx,
            lhsscan,
            rhsscan: None,
            prodscan: None,
            filename,
            layout,
            chunksize,
            nextblknum: 0,
            filesize,
        };

        scan.use_next_chunk();

        scan
    }
    fn use_next_chunk(&mut self) -> bool {
        if self.nextblknum >= self.filesize {
            return false;
        }
        if self.rhsscan.is_some() {
            let mut rhsscan = self.rhsscan.as_ref().unwrap().lock().unwrap();
            rhsscan.close().unwrap();
        }
        let mut end = self.nextblknum + self.chunksize - 1;
        if end >= self.filesize {
            end = self.filesize - 1;
        }
        let rhsscan: Arc<Mutex<dyn Scan>> = Arc::new(Mutex::new(ChunkScan::new(
            Arc::clone(&self.tx),
            &self.filename,
            Arc::clone(&self.layout),
            self.nextblknum,
            end,
        )));
        self.rhsscan = Some(Arc::clone(&rhsscan));
        self.lhsscan.lock().unwrap().before_first().unwrap();
        let prodscan = ProductScan::new(Arc::clone(&self.lhsscan), rhsscan);
        self.prodscan = Some(Arc::new(Mutex::new(prodscan)));
        self.nextblknum = end + 1;

        true
    }
}

impl Scan for MultibufferProductScan {
    fn before_first(&mut self) -> Result<()> {
        self.nextblknum = 0;
        self.use_next_chunk();

        Ok(())
    }
    fn next(&mut self) -> bool {
        while self.prodscan.is_none() || !self.prodscan.as_ref().unwrap().lock().unwrap().next() {
            if !self.use_next_chunk() {
                return false;
            }
        }

        true
    }
    fn get_i16(&mut self, fldname: &str) -> Result<i16> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_i16(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn get_i32(&mut self, fldname: &str) -> Result<i32> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_i32(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn get_string(&mut self, fldname: &str) -> Result<String> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_string(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn get_bool(&mut self, fldname: &str) -> Result<bool> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_bool(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn get_date(&mut self, fldname: &str) -> Result<NaiveDate> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_date(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn get_val(&mut self, fldname: &str) -> Result<Constant> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().get_val(fldname),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }
    fn has_field(&self, fldname: &str) -> bool {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().has_field(fldname),
            None => false,
        }
    }
    fn close(&mut self) -> Result<()> {
        match self.prodscan.as_ref() {
            Some(prodscan) => prodscan.lock().unwrap().close(),
            None => Err(From::from(MultibufferProductScanError::NoProductScan)),
        }
    }

    fn to_update_scan(&mut self) -> Result<&mut dyn UpdateScan> {
        Err(From::from(MultibufferProductScanError::DowncastError))
    }
    fn as_table_scan(&mut self) -> Result<&mut TableScan> {
        Err(From::from(MultibufferProductScanError::DowncastError))
    }
    fn as_sort_scan(&mut self) -> Result<&mut SortScan> {
        Err(From::from(MultibufferProductScanError::DowncastError))
    }
}
