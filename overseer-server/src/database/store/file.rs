use std::path::Path;

use monoio::fs::{File, OpenOptions};
use overseer::error::NetworkError;

pub const MAGIC_BYTE: u8 = 0x83;
pub const PAGE_SIZE: u32 = 16384;
pub const RESERVED_HEADER_SIZE: u32 = 512;


pub const PAGE_HEADER_RESERVED_BYTES: u32 = 1 + 4; // Is free + Previous
pub const PAGE_FOOTER_RESERVED_BYTES: u32 = 4;

pub struct PagedFile {
    underlying: File,
    file_size: u64,
    is_initialized: bool,
    free_list: Vec<RawPageAddress>
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
            object.reserve(RawPageAddress::zero(), RESERVED_HEADER_SIZE, RawPageAddress::zero()).await?;
            format_pagefile_header(&object, &mut object.header_page()).await?;

            object.is_initialized = true;
        } else {
            // Initialize the free-list
            for i in 0..object.pages() {
                let addr = RawPageAddress::new(RESERVED_HEADER_SIZE + (i * PAGE_SIZE));
                let (r, b) = object.underlying.read_exact_at(vec![0u8], addr.as_u64()).await;
                r?;
                if b[0] == 1 {
                    object.free_list.push(addr);
                }
            }
        }
        Ok(object)
        

    }
    fn header_page(&self) -> Page {
        Page {
            // file: self,
            size: RESERVED_HEADER_SIZE,
            start: RawPageAddress::new(0),
            free: true,
            next: RawPageAddress::zero(),
            previous: RawPageAddress::zero()
        }
    }
    fn pages(&self) -> u32 {
        if !self.is_initialized {
            0
        } else {
            ((self.file_size as u32) - RESERVED_HEADER_SIZE) / PAGE_SIZE
        }
    }
    pub fn free_pages(&self) -> usize {
        self.free_list.len()
    }
    async fn acquire(&self, page: u32) -> Result<Page, NetworkError> {
        if page >= self.pages() {
            Err(NetworkError::PageOutOfBounds)?;
        }
        let acked = load_page(self, RawPageAddress::new((RESERVED_HEADER_SIZE) + ((page) * PAGE_SIZE)), PAGE_SIZE).await?;
        if acked.free {
            return Err(NetworkError::PageFreedError);
        }
        Ok(acked)
    }
    async fn new_page(&mut self) -> Result<Page, NetworkError>
    {
        if self.free_list.is_empty() {
            // If the free list is empty we have to make a new page from scratch.
            self.reserve(RawPageAddress::new((RESERVED_HEADER_SIZE + (self.pages() * PAGE_SIZE)) as u32), PAGE_SIZE, RawPageAddress::zero()).await
        } else {
            // Let us reuse a page.
            let to_use = self.free_list.pop().unwrap();
            self.reserve(to_use, PAGE_SIZE, RawPageAddress::zero()).await
        }
        
    }
    async fn reserve<'a>(&'a mut self, ptr: RawPageAddress, size: u32, previous: RawPageAddress) -> Result<Page, NetworkError> {
        if self.pages() == 0 {
            self.file_size += size as u64;
            println!()
        } else if self.pages() > 0 && (ptr.as_u64() as u32 - RESERVED_HEADER_SIZE) / PAGE_SIZE >= self.pages() {
            self.file_size += size as u64;
        }
        let mut page = Page {
            // file: self,
            start: ptr,
            size,
            free: false,
            previous,
            next: RawPageAddress::zero()
        };
        format_page(&self, &mut page).await?;
        
        Ok(page)
    }
    pub async fn sync(&self) -> Result<(), NetworkError> {
        self.underlying.sync_all().await?;
        Ok(())
    }
}


pub struct PageFileHeader {
    magic: u32,
    major: u32,
    minor: u32,
    patch: u32
}

#[derive(Debug)]
pub struct Page {
    start: RawPageAddress,
    size: u32,
    free: bool,
    previous: RawPageAddress,
    next: RawPageAddress
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
struct RawPageAddress(u32);

impl RawPageAddress {
    pub fn new(inner: u32) -> Self {
        Self(inner)
    }
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
    pub fn as_u64(&self) -> u64 {
        self.0 as u64
    }
    pub fn zero() -> Self {
        Self::new(0)
    }
    pub fn offset(self, o: u32) -> Self
    {
        Self(self.0 + o)
    }
    pub fn offset_subtract(self, o: u32) -> Self {
        Self(self.0 - o)
    }
    pub fn page_number(&self) -> u32 {
        if self.is_zero() {
            0
        } else {
            (self.0 - RESERVED_HEADER_SIZE) / PAGE_SIZE
        }
        
    }
}

#[derive(Copy, Clone)]
pub enum PageType {
    Normal
}

async fn format_pagefile_header(file: &PagedFile, page: &Page) -> Result<(), NetworkError>
{
    page.write(file, 0, vec![MAGIC_BYTE, 0, 0, 0]).await.unwrap();


    Ok(())
}

async fn load_page(file: &PagedFile, ptr: RawPageAddress, size: u32) -> Result<Page, NetworkError>
{
    let (r, e) = file.underlying.read_exact_at(vec![0u8; 5], ptr.as_u64()).await;
    r?;
    let is_free = e[0] == 1;
    let previous_page = RawPageAddress::new(u32::from_le_bytes(e[1..5].try_into().unwrap()) * (PAGE_SIZE as u32) + (RESERVED_HEADER_SIZE as u32));
    
    let (r, e) = file.underlying.read_exact_at(vec![0u8; 4], ptr.offset(size - 4).as_u64()).await;
    r?;
    let next_page = RawPageAddress::new(u32::from_le_bytes(e[0..4].try_into().unwrap()) * (PAGE_SIZE as u32) + (RESERVED_HEADER_SIZE as u32));
    



    Ok(Page {
        // file,
        start: ptr,
        size,
        free: is_free,
        next: next_page,
        previous: previous_page
    })
}

async fn format_page(file: &PagedFile, page: &mut Page) -> Result<(), NetworkError>
{
    // Zero the page.
    page.raw_write(file, 0, vec![0u8; page.size as usize]).await?;

    // Write the free byte. (we are obviously free)
    page.write(file, 0, vec![0u8]).await?;

    page.next = RawPageAddress::zero();
    page.previous = RawPageAddress::zero();
    
    Ok(())
}

impl Page {
    fn bound_check(&self, position: u32, size: u32) -> Result<(), NetworkError> {
        if (position + size) > self.size {
            return Err(NetworkError::IllegalRead)?;
        }
        Ok(())
    }
    async fn raw_read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position, size)?;
        let (r, buf) = file.underlying.read_exact_at(Vec::with_capacity(size as usize), self.start.offset(position).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, size + PAGE_FOOTER_RESERVED_BYTES)?;
        let (r, buf) = file.underlying.read_exact_at(Vec::with_capacity(size as usize), self.start.offset(position).offset(PAGE_HEADER_RESERVED_BYTES as u32).as_u64()).await;
        r?;
        Ok(buf)
    }
    async fn raw_write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position, bytes.len() as u32)?;
        let (r, buf) = file.underlying.write_all_at(bytes, self.start.offset(position as u32).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, bytes.len() as u32 + PAGE_FOOTER_RESERVED_BYTES)?;
        let (r, buf) = file.underlying.write_all_at(bytes, self.start.offset(position as u32).offset(PAGE_HEADER_RESERVED_BYTES as u32).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn free(&mut self, file: &mut PagedFile) -> Result<(), NetworkError> {
        self.raw_write(file, 0, vec![1u8]).await?;
        self.free = true;
        file.free_list.push(self.start);
        Ok(())
    }
    async fn set_previous(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, 1, previous.to_le_bytes().to_vec()).await?;
        self.previous = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE);
        Ok(())
    }
    async fn set_next(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, self.size - 4 as u32, previous.to_le_bytes().to_vec()).await?;
        self.next = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE);
        Ok(())
    }
    /// Checks if this page has a next.
    pub fn has_next(&self) -> bool {
        !self.next.is_zero()
    }
    /// This will allocate a new page if we do not have a next page.
    pub async fn get_next(&mut self, file: &mut PagedFile) -> Result<Page, NetworkError> {
        if self.has_next() {
            // // We already have a page.
            Ok(file.acquire(self.next.page_number()).await?)
        } else {
            
            let mut o: Page = file.new_page().await?;
            o.set_previous(file, self.start.page_number()).await?;
            self.set_next(file, o.start.page_number()).await?;
            Ok(o)
        }
    }
    pub async fn capacity(&self) -> u32 {
        self.size - PAGE_HEADER_RESERVED_BYTES - PAGE_FOOTER_RESERVED_BYTES
    }

    
}


#[cfg(test)]
mod tests {
    use overseer::error::NetworkError;
    use tempfile::tempdir;

    use crate::database::store::file::{PAGE_SIZE, RESERVED_HEADER_SIZE};

    use super::PagedFile;


    #[monoio::test]
    pub async fn checkout() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert_eq!(paged.file_size as u32, RESERVED_HEADER_SIZE);
        paged.new_page().await.unwrap();
        paged.new_page().await.unwrap();

        assert_eq!(paged.file_size as u32, RESERVED_HEADER_SIZE + PAGE_SIZE + PAGE_SIZE);

        let mut page = paged.acquire(0).await.unwrap();
        page.write(&paged, 0, vec![1,2,3]).await.unwrap();
        assert_eq!(&page.read(&paged, 0, 3).await.unwrap(), &[1,2,3]);

        let paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert_eq!(&page.read(&paged, 0, 3).await.unwrap(), &[1,2,3]);
    }

    #[monoio::test]
    pub async fn free_page() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        paged.new_page().await.unwrap();
        assert_eq!(paged.free_pages(), 0);

        let mut page = paged.acquire(0).await.unwrap();
        assert!(!page.free);
        page.free(&mut paged).await.unwrap();
        assert!(page.free);
        assert_eq!(paged.free_pages(), 1);

 

        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        assert!(page.free);
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

        assert_eq!(page.start.page_number(), 0);
        assert!(!page.has_next());
        assert!(page.previous.is_zero());

        let chain = page.get_next(&mut paged).await.unwrap();
        assert!(page.previous.is_zero());
        assert!(page.has_next());
        assert_eq!(page.get_next(&mut paged).await.unwrap().start.page_number(), chain.start.page_number());
        assert_eq!(chain.previous.page_number(), 0);
        assert_eq!(page.next.page_number(), 1);

        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let page_a = paged.acquire(0).await.unwrap();
        let page_b = paged.acquire(1).await.unwrap();
        assert_eq!(page_a.next.page_number(), 1);
        assert_eq!(page_b.previous.page_number(), 0);
        
    }
}