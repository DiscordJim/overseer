use std::{future::Future, marker::PhantomData, ops::{Deref, DerefMut, Index, IndexMut}, slice::SliceIndex};

use overseer::{error::NetworkError, models::LocalReadAsync};

use crate::database::store::file::{PagedFile, MAGIC_BYTE, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE, RESERVED_HEADER_SIZE};

use super::{leaf_page::Leaf, meta::{PageType, RawPageAddress}};

#[derive(Debug)]
/// This is a page that has not been loaded into
/// memory, it essentially allows us the ability
/// to load it in if we need it.
pub struct PageReference {
    /// The pointer to the start of the page.
    pointer: RawPageAddress,
    /// The page size.
    size: u32
}

#[derive(Debug)]
pub struct Page {
    pub reference: PageReference,
    pub metadata: PageMetadata,
    pub backing: Box<[u8]>
}

#[derive(Debug)]
pub struct PageMetadata {
    pub free: bool,
    pub previous: RawPageAddress,
    pub next: RawPageAddress,
    pub page_type: PageType
}


impl PageReference {
    /// Creates a new reference.
    pub fn new(pointer: RawPageAddress, size: u32) -> Self {
        Self {
            pointer,
            size
        }
    }
    /// Turns the page reference into a memory loaded page.
    pub async fn load(self, page_file: &PagedFile) -> Result<Page, NetworkError> {
        let page = load_page(self, page_file).await?;
        Ok(page)
    }
    /// Formats and loads the address.
    pub async fn load_formatted(self, page_file: &PagedFile) -> Result<Page, NetworkError> {
        let (error, buffer) = page_file.handle().write_all_at(vec![0u8; self.size as usize], self.pointer.as_u64()).await;
        println!("Wrote a whole buffer at {} @ {}", self.size, self.pointer.as_u64());
        error?;
        Ok(Page {
            reference: self,
            backing: buffer.into_boxed_slice(),
            metadata: PageMetadata {
                free: false,
                next: RawPageAddress::zero(),
                previous: RawPageAddress::zero(),
                page_type: PageType::Normal
            }
        })
    }
}





async fn load_page(PageReference { pointer, size }: PageReference, page_file: &PagedFile) -> Result<Page, NetworkError>
{
    println!("Loading a page of size {size} that starts at {pointer:?}");
    let (error, backing) = page_file.handle().read_exact_at(vec![0u8; size as usize], pointer.as_u64()).await;
    error?; // propagate.
    let backing: Box<[u8]> = backing.into_boxed_slice();

    let is_free = backing[0] == 1;
    let previous_page = RawPageAddress::new(u32::from_le_bytes(backing[1..5].try_into().unwrap()) * (PAGE_SIZE as u32) + (RESERVED_HEADER_SIZE as u32));
    let next_page = RawPageAddress::new(u32::from_le_bytes(backing[5..9].try_into().unwrap()) * (PAGE_SIZE as u32) + (RESERVED_HEADER_SIZE as u32));
    let page_type = PageType::from_u8(backing[9])?;
    // println!("loaded {:?}", backing);


    Ok(Page {
        reference: PageReference { pointer, size },
        metadata: PageMetadata {
            free: is_free,
        next: next_page,
        previous: previous_page,
        page_type
        },
        backing: backing
    })
}

// async fn format_page(file: &PagedFile, page: &mut Page) -> Result<(), NetworkError>
// {
//     // Zero the page.
//     page.raw_write(file, 0, vec![0u8; page.reference.size as usize]).await?;

//     // Write the free byte. (we are obviously free)
//     // page.write(file, 0, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8]).await?;
//     page.metadata.free = false;
//     page.metadata.next = RawPageAddress::zero();
//     page.metadata.previous = RawPageAddress::zero();

    
//     Ok(())
// }

impl Page {
    pub fn start(&self) -> RawPageAddress {
        self.reference.pointer
    }
    pub fn project<P>(self) -> Projection<P> {
        Projection {
            page: self,
            projection: PhantomData
        }
    }
    pub fn normal(self) -> Projection<()> {
        self.project()
    }
    pub fn leaf(self) -> Projection<Leaf> {
        self.project()
    }
    pub async fn reload(self, page_file: &PagedFile) -> Result<Page, NetworkError> {
        Ok(self.reference.load(page_file).await?)
    }

    pub fn get_write_ptr(&self, position: u32) -> RawPageAddress {
        self.start().offset(position).offset(PAGE_HEADER_RESERVED_BYTES as u32)
    }
    fn bound_check(&self, position: u32, size: u32) -> Result<(), NetworkError> {
        if (position + size) > self.size() {
            return Err(NetworkError::IllegalRead)?;
        }
        Ok(())
    }
    // pub async fn format(self, file: &PagedFile) ->Result<Self, NetworkError> {
    //     // format_page(file, self).await?;
    //     let mut open = self.open();
    //     open.format();
    //     Ok(open.commit(file).await?)
    // }
    // async fn raw_read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
    //     self.bound_check(position, size)?;
    //     let (r, buf) = file.handle().read_exact_at(Vec::with_capacity(size as usize), self.start().offset(position).as_u64()).await;
    //     r?;
    //     Ok(buf)
    // }
    // pub async fn read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
    //     self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, size)?;
    //     let (r, buf) = file.handle().read_exact_at(Vec::with_capacity(size as usize), self.get_write_ptr(position).as_u64()).await;
    //     r?;
    //     Ok(buf)
    // }
    pub async fn raw_write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position, bytes.len() as u32)?;
        let (r, buf) = file.handle().write_all_at(bytes, self.start().offset(position as u32).as_u64()).await;
        r?;
        Ok(buf)
    }

    // pub async fn sync(&mut self, file: &PagedFile) -> Result<(), NetworkError> {
    //     let (r, buf) = file.handle().write_all_at(self.backing.take().unwrap(), self.start().as_u64()).await;
    //     r?;
    //     self.backing = Some(buf);
    //     Ok(())
    // }
    
    pub async fn write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, bytes.len() as u32)?;
        let (r, buf) = file.handle().write_all_at(bytes, self.get_write_ptr(position).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn free(&mut self, file: &mut PagedFile) -> Result<(), NetworkError> {
        self.raw_write(file, 0, vec![1u8]).await?;
        self.metadata.free = true;
        file.add_to_free_list(self.start());
        Ok(())
    }
    pub async fn get_type(&self, file: &mut PagedFile) -> Result<PageType, NetworkError> {
        let ntype = self.backing[9];
        PageType::from_u8(ntype)
    }
    // pub async fn set_type(&self, ptype: PageType, file: &mut PagedFile) -> Result<(), NetworkError> {
    //     self.raw_write(file, 9, vec![ptype.as_u8()]).await?;
    //     Ok(())
    // }
    pub async fn set_previous(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, 1, previous.to_le_bytes().to_vec()).await?;
        self.metadata.previous = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE as u32);
        Ok(())
    }
    pub async fn set_next(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, 5, previous.to_le_bytes().to_vec()).await?;
        self.metadata.next = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE as u32);
        Ok(())
    }
    /// Checks if this page has a next.
    pub fn has_next(&self) -> bool {
        !self.metadata.next.is_zero()
    }
    pub fn size(&self) -> u32 {
        self.reference.size
    }
    /// This will allocate a new page if we do not have a next page.
    pub async fn get_next(&mut self, file: &mut PagedFile) -> Result<Page, NetworkError> {
        if self.has_next() {
            // // We already have a page.
            Ok(file.acquire(self.metadata.next.page_number()).await?)
        } else {
            
            let mut o: Page = file.new_page().await?;
            o.set_previous(file, self.start().page_number()).await?;
            self.set_next(file, o.start().page_number()).await?;
            Ok(o)
        }
    }

    pub fn capacity(&self) -> u32 {
        self.size() - PAGE_HEADER_RESERVED_BYTES 
    }
    // pub async fn view(&self) -> Result<&[u8], NetworkError> {
    //     Ok(self.raw_read(file, 0, PAGE_SIZE as u32).await?.try_into().unwrap())
    // }
    // pub async fn hexdump(&self, file: &PagedFile) -> Result<String, NetworkError> {
    //     const WIDTH: usize = 8;
    //     let view = self.view(file).await?;

    //     let rows = view.len() / WIDTH;
    //     for y in 0..rows {
    //         print!("{:04x}\t", (y as u16 * WIDTH as u16) as u16);
    //         for x in 0..WIDTH {
                
    //             print!("{:02x} ", view[y * WIDTH + x]);
    //         }
    //         println!("");
    //     }
    //     Ok(String::new())
    // }
    pub fn end(&self) -> RawPageAddress {
        self.start().offset(self.size())
    }

    
}

// impl Index<usize> for Page {
//     type Output = u8;
//     fn index(&self, index: usize) -> &Self::Output {
//         self.backing.index(index + PAGE_HEADER_RESERVED_BYTES as usize)
//     }
// }

impl<P, T> Index<T> for Projection<P>
where 
    T: SliceIndex<[u8]>
{
    type Output = T::Output;
    fn index(&self, index: T) -> &Self::Output {
        let o = &(self.page.backing)[PAGE_HEADER_RESERVED_BYTES as usize..];
        o.index(index)
    }
}




// impl Deref for Page {
//     type Target = [u8];
//     fn deref(&self) -> &Self::Target {
//         &self.backing
//     }
// }

pub struct ProjReader<'a, P> {
    proj: &'a Projection<P>,
    position: usize
}

#[async_trait::async_trait(?Send)]
impl<P> LocalReadAsync for ProjReader<'_, P> {
    async fn read_exact(&mut self, mut buffer: Vec<u8>) -> std::io::Result<(Vec<u8>, usize)> {
        let length = buffer.len();
        buffer.copy_from_slice(&self.proj[self.position..self.position + length]);
        // let (error, buffer) = self.underlying.underlying.read_exact_at(buffer, self.position as u64).await;
        // error?; // propagate.
        
        self.position += length;
        Ok((buffer, length))
    }
}

pub struct Projection<P> {
    page: Page,
    projection: PhantomData<P>
}

pub struct Transact<K> {
    page: Projection<K>
    // _type: PhantomData<K>s
}


impl<P> Projection<P> {
    fn transact(self) -> Transact<P> {
        Transact {
            page: self
        }
    }
    pub async fn open<F>(self, page_file: &PagedFile, functor: F) -> Result<Self, NetworkError>
    where 
        F: AsyncFnOnce(&mut Transact<P>) -> Result<(), NetworkError>

    {
  
        let mut transacting = self.transact();

        (functor)(&mut transacting).await?;

        let page = transacting.commit(page_file).await?;

        Ok(page)


    }
}

impl<P> Projection<P> {
    pub async fn reload(self, file: &PagedFile) -> Result<Self, NetworkError>
    {
        Ok(Self {
            page: self.page.reference.load(file).await?,
            projection: PhantomData
        })
    }
    pub fn reader(&self, position: usize) -> ProjReader<P> {
        ProjReader {
            position,
            proj: self
        }
    }
    pub fn previous(&self) -> RawPageAddress {
        self.page.metadata.previous
    }
    pub fn next(&self) -> RawPageAddress {
        self.page.metadata.next
    }
    pub fn is_free(&self) -> bool {
        self.page.metadata.free
    }
    pub fn size(&self) -> usize {
        self.page.reference.size as usize
    }
    pub fn capacity(&self) -> usize {
        self.size() - PAGE_HEADER_RESERVED_BYTES as usize
    }
    pub fn page_type(&self) -> PageType {
        self.page.metadata.page_type
    }
}

impl<K> Transact<K> {
    pub async fn commit(self, file: &PagedFile) -> Result<Projection<K>, NetworkError> {
        let (r, buf) = file.handle().write_all_at(self.page.page.backing, self.page.page.reference.pointer.as_u64()).await;
        r?;
        Ok(Projection {
            page: Page {
                metadata: self.page.page.metadata,
                backing: buf,
                reference: self.page.page.reference
            },
            projection: PhantomData
        })
    }
    pub fn set_free(&mut self, is_free: bool) {
        if is_free {
            self.page.page.backing[0] = 1;
        } else {
            self.page.page.backing[0] = 0;
        }
        self.page.page.metadata.free = is_free;
    }
    pub fn set_previous(&mut self, previous: Option<u32>) {
        match previous {
            Some(previous) => {
                // Set the actual pointer in the backing buffer.
                self.page.page.backing[1..5].copy_from_slice(&previous.to_le_bytes());
                self.page.page.metadata.previous = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE as u32);
            },
            None => {
                self.page.page.backing[1..5].fill(0);
                self.page.page.metadata.previous = RawPageAddress::zero();
            }
        }
        
        
    }
    pub fn set_type(&mut self, ptype: PageType) {
        self.page.page.metadata.page_type = ptype;
        self.page.page.backing[9] = ptype.as_u8();
    }
    // pub async fn set_free(&mut self)
    pub fn set_next(&mut self, next: Option<u32>) {
        match next {
            Some(next) => {
                // Set the actual pointer in the backing buffer.
                self.page.page.backing[5..9].copy_from_slice(&next.to_le_bytes());
                self.page.page.metadata.previous = RawPageAddress::new(RESERVED_HEADER_SIZE + next * PAGE_SIZE as u32);
            },
            None => {
                self.page.page.backing[5..9].fill(0);
                self.page.page.metadata.previous = RawPageAddress::zero();
            }
        }
    }

    pub fn format(&mut self) {
        self.set_free(false);
        self.set_previous(None);
        self.set_next(None);
        self.page.page.backing.fill(0);

        
    }
}

impl<P> Deref for Transact<P> {
    type Target = Projection<P>;
    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

// pub struct OpenPage {
//     metadata: PageMetadata,
//     reference: PageReference,
//     backing: Box<[u8]>,
// }

// impl OpenPage {
//     pub fn open(page: Page) -> OpenPage {
//         OpenPage {
//             backing: page.backing,
//             metadata: page.metadata,
//             reference: page.reference
//         }
//     }
    

//     pub async fn commit(self, file: &PagedFile) -> Result<Page, NetworkError> {
//         let (r, buf) = file.handle().write_all_at(self.backing, self.reference.pointer.as_u64()).await;
//         r?;
//         Ok(Page {
//             metadata: self.metadata,
//             backing: buf,
//             reference: self.reference
//         })
//     }
// }

impl<K, T> Index<T> for Transact<K>
where 
    T: SliceIndex<[u8]>
{
    type Output = T::Output;
    fn index(&self, index: T) -> &Self::Output {
        let o = &(self.page.page.backing)[PAGE_HEADER_RESERVED_BYTES as usize..];
        o.index(index)
    }
}


impl<K, T> IndexMut<T> for Transact<K>
where 
    T: SliceIndex<[u8]>
{
    fn index_mut(&mut self, index: T) -> &mut Self::Output {
        self.page.page.backing[PAGE_HEADER_RESERVED_BYTES as usize..].index_mut(index)
    }
}


// impl Drop for OpenPage {
//     fn drop(&mut self) {
//         if !self.finished {
//             panic!("Dropped a protected page writer without actually comitting it.")
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::database::store::file::PagedFile;


    #[monoio::test]
    async fn test_open_page_committal() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let page =  paged.new_page().await.unwrap().project::<()>();

        assert_eq!(&page[0..4], &[0, 0, 0, 0]);

        let mut open = page.transact();
        open[0..4].copy_from_slice(&[1,2,3,4]);
        let page = open.commit(&paged).await.unwrap();
        assert_eq!(&page[0..4], &[1,2,3,4]);


        let page = page.reload(&paged).await.unwrap();
        assert_eq!(&page[0..4], &[1,2,3,4]);

    }

    #[monoio::test]
    async fn test_open_page_committal_metadata() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let page =  paged.new_page().await.unwrap().project::<()>();

        assert_eq!(&page[0..4], &[0, 0, 0, 0]);

        let mut open = page.transact();
        open[0..4].copy_from_slice(&[1,2,3,4]);
        open.set_previous(Some(24));
        let page = open.commit(&paged).await.unwrap();
       
        assert_eq!(&page[0..4], &[1,2,3,4]);
        assert_eq!(page.previous().page_number(), 24);


        let page = page.reload(&paged).await.unwrap();
        assert_eq!(&page[0..4], &[1,2,3,4]);

    }
}