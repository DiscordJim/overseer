use overseer::error::NetworkError;

use crate::database::store::file::{PagedFile, MAGIC_BYTE, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE, RESERVED_HEADER_SIZE};

use super::meta::{PageType, RawPageAddress};

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
    pub free: bool,
    pub previous: RawPageAddress,
    pub next: RawPageAddress,
    pub backing: Box<[u8]>
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
            free: false,
            next: RawPageAddress::zero(),
            previous: RawPageAddress::zero()
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

    println!("loaded {:?}", backing);


    Ok(Page {
        reference: PageReference { pointer, size },
        free: is_free,
        next: next_page,
        previous: previous_page,
        backing
    })
}

async fn format_page(file: &PagedFile, page: &mut Page) -> Result<(), NetworkError>
{
    // Zero the page.
    page.raw_write(file, 0, vec![0u8; page.reference.size as usize]).await?;

    // Write the free byte. (we are obviously free)
    // page.write(file, 0, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8]).await?;
    page.free = false;
    page.next = RawPageAddress::zero();
    page.previous = RawPageAddress::zero();

    
    Ok(())
}

impl Page {
    pub fn start(&self) -> RawPageAddress {
        self.reference.pointer
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
    pub async fn format(&mut self, file: &PagedFile) ->Result<(), NetworkError> {
        format_page(file, self).await?;
        Ok(())
    }
    async fn raw_read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position, size)?;
        let (r, buf) = file.handle().read_exact_at(Vec::with_capacity(size as usize), self.start().offset(position).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn read(&self, file: &PagedFile, position: u32, size: u32) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, size)?;
        let (r, buf) = file.handle().read_exact_at(Vec::with_capacity(size as usize), self.get_write_ptr(position).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn raw_write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position, bytes.len() as u32)?;
        let (r, buf) = file.handle().write_all_at(bytes, self.start().offset(position as u32).as_u64()).await;
        r?;
        Ok(buf)
    }
    
    pub async fn write(&self, file: &PagedFile, position: u32, bytes: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        self.bound_check(position + PAGE_HEADER_RESERVED_BYTES, bytes.len() as u32)?;
        let (r, buf) = file.handle().write_all_at(bytes, self.get_write_ptr(position).as_u64()).await;
        r?;
        Ok(buf)
    }
    pub async fn free(&mut self, file: &mut PagedFile) -> Result<(), NetworkError> {
        self.raw_write(file, 0, vec![1u8]).await?;
        self.free = true;
        file.add_to_free_list(self.start());
        Ok(())
    }
    pub async fn get_type(&self, file: &mut PagedFile) -> Result<PageType, NetworkError> {
        let ntype = self.raw_read(file, 9, 1).await?[0];
        PageType::from_u8(ntype)
    }
    pub async fn set_type(&self, ptype: PageType, file: &mut PagedFile) -> Result<(), NetworkError> {
        self.raw_write(file, 9, vec![ptype.as_u8()]).await?;
        Ok(())
    }
    pub async fn set_previous(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, 1, previous.to_le_bytes().to_vec()).await?;
        self.previous = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE as u32);
        Ok(())
    }
    pub async fn set_next(&mut self, file: &PagedFile, previous: u32) -> Result<(), NetworkError> {
        self.raw_write(file, 5, previous.to_le_bytes().to_vec()).await?;
        self.next = RawPageAddress::new(RESERVED_HEADER_SIZE + previous * PAGE_SIZE as u32);
        Ok(())
    }
    /// Checks if this page has a next.
    pub fn has_next(&self) -> bool {
        !self.next.is_zero()
    }
    pub fn size(&self) -> u32 {
        self.reference.size
    }
    /// This will allocate a new page if we do not have a next page.
    pub async fn get_next(&mut self, file: &mut PagedFile) -> Result<Page, NetworkError> {
        if self.has_next() {
            // // We already have a page.
            Ok(file.acquire(self.next.page_number()).await?)
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
    pub async fn view(&self, file: &PagedFile) -> Result<[u8; PAGE_SIZE], NetworkError> {
        Ok(self.raw_read(file, 0, PAGE_SIZE as u32).await?.try_into().unwrap())
    }
    pub async fn hexdump(&self, file: &PagedFile) -> Result<String, NetworkError> {
        const WIDTH: usize = 8;
        let view = self.view(file).await?;

        let rows = view.len() / WIDTH;
        for y in 0..rows {
            print!("{:04x}\t", (y as u16 * WIDTH as u16) as u16);
            for x in 0..WIDTH {
                
                print!("{:02x} ", view[y * WIDTH + x]);
            }
            println!("");
        }
        Ok(String::new())
    }
    pub fn end(&self) -> RawPageAddress {
        self.start().offset(self.size())
    }

    
}

