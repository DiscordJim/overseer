use std::{fmt::UpperHex, io, path::Path};

use monoio::fs::{File, OpenOptions};
use overseer::{error::NetworkError, models::{asynctrait, IoBufferMut, LocalReadAsync}};

use super::paging::{meta::{PageType, RawPageAddress}, page::{Page, PageReference}};



pub const MAGIC_BYTE: u8 = 0x83;
pub const PAGE_SIZE: usize = 4096;
pub const RESERVED_HEADER_SIZE: u32 = 4096;


pub const PAGE_HEADER_RESERVED_BYTES: u32 = 1 + 4 + 4 + 1; // Is free + Previous
// pub const PAGE_FOOTER_RESERVED_BYTES: u32 = 4;

pub struct PagedFile {
    underlying: File,
    file_size: u64,
    is_initialized: bool,
    free_list: Vec<RawPageAddress>
}


pub struct PagedFileRw<'a> {
    position: usize,
    underlying: &'a PagedFile
}

#[async_trait::async_trait(?Send)]
impl LocalReadAsync for PagedFileRw<'_> {
    async fn read_exact(&mut self, buffer: Vec<u8>) -> std::io::Result<(Vec<u8>, usize)> {
        let (error, buffer) = self.underlying.underlying.read_exact_at(buffer, self.position as u64).await;
        error?; // propagate.
        let length = buffer.len();
        self.position += length;
        Ok((buffer, length))
    }
}



impl PagedFile {
    pub async fn open<P>(path: P) -> Result<Self, NetworkError>
    where 
        P: AsRef<Path>
    {


        let file = OpenOptions::new().create(true).read(true).truncate(false).write(true).open(path.as_ref()).await?;
        let size = file.metadata().await?.len();
        let mut object = Self {
            underlying: file,
            file_size: size,
            is_initialized: size != 0,
            free_list: Vec::new()
        };
        
        
        
        if !object.is_initialized {
            // Initialize the database.
            let mut page = object.reserve(RawPageAddress::zero(), RESERVED_HEADER_SIZE, None).await?;
            format_pagefile_header(&object, page).await?;

            object.is_initialized = true;
        } else {
            // Initialize the free-list
            for i in 0..object.pages() {
                let addr = RawPageAddress::new(RESERVED_HEADER_SIZE + (i * PAGE_SIZE as u32));
                let (r, b) = object.underlying.read_exact_at(vec![0u8], addr.as_u64()).await;
                r?;
                if b[0] == 1 {
                    object.free_list.push(addr);
                }
            }
        }
        Ok(object)
        

    }
    pub fn add_to_free_list(&mut self, addr: RawPageAddress) {
        self.free_list.push(addr)
    }
    pub fn reader(&self, position: usize) -> PagedFileRw<'_> {
        PagedFileRw {
            position,
            underlying: self
        }
    }
    pub fn handle(&self) -> &File {
        &self.underlying
    }
    fn header_page(&self) -> PageReference {
        PageReference::new(RawPageAddress::zero(), RESERVED_HEADER_SIZE)
        // Page {
        //     // file: self,
        //     reference:
        //     size: RESERVED_HEADER_SIZE,
        //     start: RawPageAddress::new(0),
        //     free: true,
        //     next: RawPageAddress::zero(),
        //     previous: RawPageAddress::zero()
        // }
    }
    fn pages(&self) -> u32 {
        if !self.is_initialized {
            0
        } else {
            ((self.file_size as u32) - RESERVED_HEADER_SIZE) / PAGE_SIZE as u32
        }
    }
    pub fn free_pages(&self) -> usize {
        self.free_list.len()
    }
    pub async fn acquire(&self, page: u32) -> Result<Page, NetworkError> {
        if page >= self.pages() {
            Err(NetworkError::PageOutOfBounds)?;
        }
        let addr = RawPageAddress::new((RESERVED_HEADER_SIZE) + ((page) * PAGE_SIZE as u32));
        let reference = PageReference::new(addr, PAGE_SIZE as u32);
        // println!("Reference: {:#?}", reference);

        let acked = reference.load(self).await?;
        if acked.metadata.free {
            return Err(NetworkError::PageFreedError);
        }
        Ok(acked)
    }
    pub async fn new_page(&mut self) -> Result<Page, NetworkError>
    {
        if self.free_list.is_empty() {
            // If the free list is empty we have to make a new page from scratch.
            // println!("FOCA");
            self.reserve(RawPageAddress::new((RESERVED_HEADER_SIZE + (self.pages() * (PAGE_SIZE as u32))) as u32), PAGE_SIZE as u32, None).await
        } else {
            // Let us reuse a page.
            // println!("FOCB");
            let to_use = self.free_list.pop().unwrap();
            self.reserve(to_use, PAGE_SIZE as u32, None).await
        }
        
    }
    async fn reserve<'a>(&'a mut self, ptr: RawPageAddress, size: u32, previous: Option<RawPageAddress>) -> Result<Page, NetworkError> {
        if self.pages() == 0 {
            self.file_size += size as u64;
            // println!()
        } else if self.pages() > 0 && (ptr.as_u64() as u32 - RESERVED_HEADER_SIZE) / PAGE_SIZE as u32 >= self.pages() {
            self.file_size += size as u64;
        }

        // println!("Performing reserve. {:?}", previous);
        let reference = PageReference::new(ptr, size);
        let mut page = reference.load_formatted(self).await?;
        if let Some(previous) = previous {
            page.set_previous(self, previous.page_number()).await?;
        }
        
        // println!("Performing reserve2. {:?}", page);

        
        Ok(page)
    }
    pub async fn sync(&self) -> Result<(), NetworkError> {
        self.underlying.sync_all().await?;
        Ok(())
    }
}

async fn format_pagefile_header(file: &PagedFile, page: Page) -> Result<(), NetworkError>
{
    page.normal().open(&file, async |tx| {
        tx[0] = MAGIC_BYTE;

        Ok(())
    }).await.unwrap();

    // page.raw_write(file, 0, vec![MAGIC_BYTE, 0, 0, 0]).await.unwrap();


    Ok(())
}



#[cfg(test)]
mod tests {
    use overseer::error::NetworkError;
    use tempfile::tempdir;

    use crate::database::store::file::{PageType, RawPageAddress, PAGE_SIZE, RESERVED_HEADER_SIZE};

    use super::PagedFile;

    #[monoio::test]
    pub async fn page_types() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert_eq!(paged.file_size as u32, RESERVED_HEADER_SIZE);
        let page=  paged.new_page().await.unwrap().normal();
        assert_eq!(page.page_type(), PageType::Normal);

        let page = page.open(&paged, async |f| {
            f.set_type(PageType::Dummy);
            assert_eq!(f.page_type(), PageType::Dummy);
            Ok(())
        }).await.unwrap();

        let page = page.reload(&paged).await.unwrap();
        assert_eq!(page.page_type(), PageType::Dummy);

        // page.set_type(PageType::Dummy, &mut paged).await.unwrap();
        // assert_eq!(page.get_type(&mut paged).await.unwrap(), PageType::Dummy);

        // let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        // assert_eq!(page.get_type(&mut paged).await.unwrap(), PageType::Dummy);
        
    
    }


    #[monoio::test]
    pub async fn checkout() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert_eq!(paged.file_size as u32, RESERVED_HEADER_SIZE);
        paged.new_page().await.unwrap();
        paged.new_page().await.unwrap();

        assert_eq!(paged.file_size as usize, RESERVED_HEADER_SIZE as usize + PAGE_SIZE + PAGE_SIZE);

        let page = paged.acquire(0).await.unwrap().normal().open(&paged, async |page| {
            page[..3].copy_from_slice(&[1,2,3]);
            Ok(())
        }).await.unwrap();
        // let mut page = paged.acquire(0).await.unwrap().normal().transact();
        // page[..3].copy_from_slice(&[1,2,3]);
        // let page = page.commit(&paged).await.unwrap();


        assert_eq!(&page[..3], &[1,2,3]);

        let paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert_eq!(&page[..3], &[1,2,3]);
    }

    #[monoio::test]
    pub async fn free_page() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        paged.new_page().await.unwrap();
        assert_eq!(paged.free_pages(), 0);

        let mut page = paged.acquire(0).await.unwrap();
        assert!(!page.metadata.free);
        page.free(&mut paged).await.unwrap();
        assert!(page.metadata.free);
        assert_eq!(paged.free_pages(), 1);

 

        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert!(page.metadata.free);
        assert_eq!(paged.free_pages(), 1);

        // Check to see that free pages are recycled.
        let mut page = paged.new_page().await.unwrap();
        assert_eq!(paged.free_pages(), 0);

        // Error if we forcibly acquire a freedpage
        page.free(&mut paged).await.unwrap();
        assert!(matches!(paged.acquire(0).await.err().unwrap(), NetworkError::PageFreedError));
    }

    #[monoio::test]
    pub async fn page_chaining() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let mut page = paged.new_page().await.unwrap();

        assert_eq!(page.start().page_number(), 0);
        assert!(!page.has_next());
        println!("Page previous: {:?}", page.metadata.previous);
        assert!(page.metadata.previous.is_zero());

        let chain = page.get_next(&mut paged).await.unwrap();
        assert!(page.metadata.previous.is_zero());
        assert!(page.has_next());
        assert_eq!(page.get_next(&mut paged).await.unwrap().start().page_number(), chain.start().page_number());
        assert_eq!(chain.metadata.previous.page_number(), 0);
        assert_eq!(page.metadata.next.page_number(), 1);

        let paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let page_a = paged.acquire(0).await.unwrap();
        let page_b = paged.acquire(1).await.unwrap();
        assert_eq!(page_a.metadata.next.page_number(), 1);
        assert_eq!(page_b.metadata.previous.page_number(), 0);
        
    }

    #[monoio::test]
    pub async fn test_header_consistency() {

        // This makes sure we are reading/writing the header correctly.

        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let mut page = paged.new_page().await.unwrap();

     
        assert_eq!(page.get_type(&mut paged).await.unwrap(), PageType::Normal);
        page.set_next(&paged, 23).await.unwrap();
        assert_eq!(page.get_type(&mut paged).await.unwrap(), PageType::Normal);
        assert_eq!(page.metadata.next.as_u64(), RESERVED_HEADER_SIZE as u64 + 23 * PAGE_SIZE as u64);

        page.set_previous(&paged, 78).await.unwrap();
        assert_eq!(page.get_type(&mut paged).await.unwrap(), PageType::Normal);
        assert_eq!(page.metadata.next.as_u64(), RESERVED_HEADER_SIZE as u64 + 23 * PAGE_SIZE as u64);
        assert_eq!(page.metadata.previous.as_u64(), RESERVED_HEADER_SIZE as u64 + 78 * PAGE_SIZE as u64);





        
    }
}