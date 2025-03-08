//!
//! The B-tree page.
//! 
//! 
//! The fragmented bytes counts not only genuinely fragmented memory but also "detractive" memory which is that
//! which may be considered unnecessary, such as free list allocations.

use std::{io::Cursor, process::exit};

use overseer::{error::NetworkError, models::Value, network::{OverseerSerde, OvrInteger}};


use crate::database::store::file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE};

use super::{error::PageError, page::{Page, Projection, Transact}};


/// How many slots a free block has.
const FRAGMENTATION_SIZE: usize = 4;

pub struct Leaf;

/// The format of the leaf page starts with a cell count (2-byte)
// pub struct LeafPage {
//     inner: Page
// }

pub struct Record {
    value: Option<Value>
}


pub struct SerializedRecord {
    value: Record,
    data: Vec<u8>,
}

impl Record {
    pub async fn produce(self) -> SerializedRecord {
        let mut cursor = Cursor::new(vec![]);
        self.serialize(&mut cursor).await.unwrap();
        SerializedRecord {
            value: self,
            data: cursor.into_inner()
        }
    }
}

impl SerializedRecord {
    pub fn to_record(self) -> Record {
        self.value
    }
    pub fn total_serialized_size(&self) -> usize {
        self.data.len() + OvrInteger::required_space(self.data.len())
    }
}


impl OverseerSerde<Record> for Record {
    type E = NetworkError;
    async fn deserialize<R: overseer::models::LocalReadAsync>(reader: &mut R) -> Result<Record, Self::E> {
        let val = Option::<&Value>::deserialize(reader).await?;
        Ok(Self {
            value: val
        })
    }
    async fn serialize<W: overseer::models::LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        self.value.as_ref().serialize(writer).await?;
        Ok(())
    }
}



struct Allocation {
    location: usize,
    /// The free block bytes in case we need to zero them.
    free_block: Option<FreeBlock>,
    size: usize,
    is_lead_allocation: bool
}

#[derive(Debug)]
struct FreeBlock {
    /// Where the free block is located, this
    /// is not written nor read.
    position: usize,
    next: u16,
    offset: u16,
    size: u16
}

impl FreeBlock {
    pub const fn size() -> usize {
        2 + 2 + 2
    }
    pub fn read(position: usize, array: &Projection<Leaf>) -> Result<Self, PageError> {
        let array = &array[position..];
        let next = u16::from_le_bytes(array[0..2].try_into().map_err(|_| PageError::FailedReadingFreeBlock)?);
        let offset = u16::from_le_bytes(array[2..4].try_into().map_err(|_| PageError::FailedReadingFreeBlock)?);
        let size = u16::from_le_bytes(array[4..6].try_into().map_err(|_| PageError::FailedReadingFreeBlock)?);
        Ok(Self {
            position,
            next,
            offset,
            size
        })
    }
    pub fn write(array: &mut Transact<Leaf>, start: usize, next: usize, offset: usize, size: usize) {
        let subset = &mut array[start..];
        subset[0..2].copy_from_slice(&(next as u16).to_le_bytes());
        subset[2..4].copy_from_slice(&(offset as u16).to_le_bytes());
        subset[4..6].copy_from_slice(&(size as u16).to_le_bytes());
    }
}

impl Projection<Leaf> {
    pub fn get_cell_count(&self) -> usize {
        u16::from_le_bytes(self[0..2].try_into().unwrap()) as usize
    }
    pub fn get_used_space(&self) -> usize {
        u16::from_le_bytes(self[2..4].try_into().unwrap()) as usize
    }

    pub fn get_free_space(&self) -> usize {
        self.capacity() - Self::header_size() - self.get_used_space()
    }
    pub fn get_free_block_ptr(&self) -> usize {
        u16::from_le_bytes(self[4..6].try_into().unwrap()) as usize
    }
    pub fn get_lead_offset(&self) -> usize {
        u16::from_le_bytes(self[6..8].try_into().unwrap()) as usize
    }
    pub fn get_fragmented(&self) -> usize {
        u16::from_le_bytes(self[8..10].try_into().unwrap()) as usize
    }

    /// The header has the following structure
    /// [ Cell Count (2) ]
    /// [ Used Space (2) ]
    /// [ Free Block Ptr (2) ] - For finding space.
    /// [ Lead Offset (2) ] - For fresh allocation.
    /// [ Fragmented Bytes (2) ] - The amount of fragmentation.
    pub const fn header_size() -> usize {
        10
    }
    pub fn calculate_offset_index(index: usize) -> usize {
        Self::header_size() + index * 2
    }
    /// Reads an offset given an index.
    pub fn get_offset(&self, index: usize) -> usize {
        let pos = Self::calculate_offset_index(index);

        // let unitary = u16::from_le_bytes(self[pos .. pos + 2].try_into().unwrap()) as usize;
        // if index == 2 { println!("2 {unitary}"); exit(1) };
        u16::from_le_bytes(self[pos .. pos + 2].try_into().unwrap()) as usize
    }
    /// Checks if a record will fit into the database.
    pub fn will_fit(&self, record: &SerializedRecord) -> bool {
        let remaining = self.get_free_space();
        record.data.len() + Self::header_size() <= remaining as usize
    }

    /// Determines the size of a record on the leaf page.
    pub fn get_record_size(&self, record: usize) -> Option<usize> {
        let (_, actual) = self.get_record_size_characteristics(record)?;
        Some(actual)
    }
    /// Determines the full size of a record including the size pointer.
    pub fn get_total_record_size(&self, record: usize) -> Option<usize> {
        let (length, actual) = self.get_record_size_characteristics(record)?;
        Some(length + actual)
    }
    /// Determines the full details of a record's size on isk.
    pub fn get_record_size_characteristics(&self, record: usize) -> Option<(usize, usize)> {
        if !self.check_record_exists(record) {
            None
        } else {
            let record_size = OvrInteger::read_slice(&self[self.get_offset(record)..]).unwrap();
            Some((OvrInteger::required_space(record_size), record_size))
        }
    }

    /// Determines if an allocation can be made within a page.
    /// 
    /// If it can, then we return a pointer to the allocation location.
    pub fn can_allocate(&self, size: usize) -> Option<Allocation> {
        if size >= self.get_free_space() {
            // No allocaion is possible.
            None
        } else {
            // We need to read the free list.
            match self.find_free_slot(size) {
                // Use the free block allocation.
                Some(alloc) => Some(alloc),
                // Fall back to allocating with the lead pointr.
                None => self.try_alloc_lead(size)
            }
        }
    }

    /// Tries allocating with the lead pointer.
    pub fn try_alloc_lead(&self, size: usize) -> Option<Allocation> {
        let lead_ptr = self.capacity() - self.get_lead_offset() - size;
        let offset_size = Self::header_size() + 2 * self.get_cell_count();

        // Check if this allocation would leak into the offsets/page header.
        // To understand the calculation taking place here, if we are so large
        // that we are pushed to the same size or less then we will leak.
        //
        // TODO: Is the equal necessary?
        if lead_ptr <= offset_size {
            None
        } else {
            // Approve the allocation.
            Some(Allocation {
                free_block: None,
                size,
                location: lead_ptr,
                is_lead_allocation: true
            })
        }

    }

    /// Find suitable free block.
    fn find_free_slot(&self, size: usize) -> Option<Allocation> {
        println!("Finding a free block... for size {}", size);
        let star = self.get_free_block_ptr();
        if star == 0 {
            // We do not even have a running free block so how
            // could we use it to allocate space?
            None
        } else {
            // Search the free chain in hopes of finding a block.
            self.descend_free_chain(star, size)
        }
    }  

    /// Descend free chain
    fn descend_free_chain(&self, pointer: usize, size: usize) -> Option<Allocation> {
        
        let block = FreeBlock::read(pointer, self).ok()?;
        if block.size as usize >= size && block.offset != 0 {
            // We must always check if the offset is zero, this indicates a block that can be skipped.
            let offset = block.offset as usize;
            Some(Allocation {
                free_block: Some(block),
                size,
                is_lead_allocation: false,
                location: offset
            })
        } else if block.next == 0 {
            // You've reached the end of the chain.
            None
        } else {
            // Descend again.
            self.descend_free_chain(pointer, size)
        }
    }

    /// Reads a record if it exists.
    pub async fn read_record(&self, index: usize) -> Result<Option<Record>, PageError>
    {
        if index >= self.get_cell_count() {
            return Ok(None);
        } else {
            // Calculate the record offset.
            let offset = self.get_offset(index);

            let mut reader = self.reader(offset as usize);
            // read the size first.
            OvrInteger::read::<usize, _>(&mut reader).await?;
            // now deserialize the record.
            let record = Record::deserialize(&mut reader).await.map_err(|_| PageError::RecordDeserializationFailure)?;

            Ok(Some(record))
        }
    }

    /// Checks if a record exists.
    pub fn check_record_exists(&self, record: usize) -> bool {
        // If an offset is zero, then it would be pointing to the cell count which
        // is clearly not a valid offset.
        
        self.get_offset(record) != 0
    }


    

    // fn check_fragmented_fit(&self, offset_index: usize, space_needed: usize) -> bool {
    //     let offset = self.get_offset(offset_index);
    //     if offset == 0 {
    //         // This is a free offset.
    //     } else {
    //         false
    //     }
    // }

    /// Reads the free chain, mostly for testing.
    fn read_free_chain(&self) -> Result<Vec<FreeBlock>, PageError> {
        let mut traversal = vec![];
        let first = self.get_free_block_ptr();
        Ok(if first == 0 {
            traversal
        } else {
            let mut block = FreeBlock::read(first, self)?;

            while block.next != 0 {
                let next_ptr = block.next as usize;
                traversal.push(block);
                block = FreeBlock::read(next_ptr, self)?;
            }
            traversal.push(block);
            
            traversal
        })
    }
    
    
    /// Finds a new record pointer location in the file. This takes in the new cell count,
    /// which is the cell count including this new record.
    /// 
    /// This is just a short cut to the allocate metho.
    fn find_new_record_ptr(&self, record: &SerializedRecord) -> Option<Allocation> {
        self.can_allocate(record.total_serialized_size())
    }
    
    
}

impl Transact<Leaf> {
    /// Increments and returns the new Cell count.
    pub fn increment_cell_count(&mut self) -> usize {
        let new = self.get_cell_count() + 1;
        self.set_cell_count(new);
        new
    }
    pub fn set_cell_count(&mut self, cells: usize) {
        self[0..2].copy_from_slice(&(cells as u16).to_le_bytes());
        // self.inner.write(file, 0, cells.to_le_bytes().to_vec()).await?;
 
    }
    pub fn set_used_space(&mut self, total: usize) {
        self[2..4].copy_from_slice(&(total as u16).to_le_bytes());
    }
    pub fn set_free_ptr(&mut self, ptr: usize) {
        self[4..6].copy_from_slice(&(ptr as u16).to_le_bytes());
    }
    pub fn set_lead_offset(&mut self, ptr: usize) {
        self[6..8].copy_from_slice(&(ptr as u16).to_le_bytes());
    }
    pub fn set_fragmented(&mut self, count: usize) {
        self[8..10].copy_from_slice(&(count as u16).to_le_bytes());
    }
    /// Writes a new offset given the offset index and the
    /// actual record ptr.
    /// 
    /// This is the physical offset. 
    fn write_record_offset(&mut self, offset_index: usize, record_ptr: usize) {
        let offset = Projection::<Leaf>::header_size() + offset_index * 2;
        self[offset..offset + 2].copy_from_slice(&(record_ptr as u16).to_le_bytes());
    }
    pub async fn write_record(&mut self, record: Record) -> Result<(), PageError> {
        self.write_serialized_record(record.produce().await).await
    }
    pub async fn write_serialized_record(&mut self, record: SerializedRecord) -> Result<(), PageError> {
        if !self.will_fit(&record) {
            return Err(PageError::LeafPageFull);
        }
        // Increment and get the new cell count.
        let new_cell_count = self.increment_cell_count();

        // Find a new record pointer.
        let record_allocation = self.find_new_record_ptr(&record).ok_or_else(|| PageError::LeafPageFull)?;
        let record_ptr = record_allocation.location;
        println!("Record Ptr: {}", record_ptr);

        // let total_usage = 2 + record.total_serialized_size();
        
        let size = OvrInteger::to_bytes(record.data.len()).await;

        // Update the free space.
        
        // println!("Writing new space {}", self.get_used_space() + (2 + record.total_serialized_size()));
        // self.set_used_space(self.get_used_space() + (2 + record.total_serialized_size()));
        // println!("Writing new space {}", self.get_used_space() + (2 + record.total_serialized_size()));

        // Write the record.
        self[record_ptr..record_ptr + size.len()].copy_from_slice(&size);
        self[record_ptr + size.len()..record_ptr + record.data.len() + size.len()].copy_from_slice(&record.data);
        // self.inner.write(file, record_ptr, record.data).await?;

        // Calculate the offset.
        self.write_record_offset(new_cell_count as usize - 1, record_ptr);

        // Commit this to the lead pointer.
        // The solver does not take the offset into account, so we need to update this.
        self.set_used_space(self.get_used_space() + 2);
        self.solve_allocate(record_allocation)?;
        // self.set_lead_offset(self.get_lead_offset() + total_usage);
        
        
        Ok(())
        
    }


    // /// Allocate free block
    // /// 
    // /// This returns the pointer to the free block.
    // fn allocate_free_block(&mut self) -> Result<usize, PageError> {
    //     let total_free_block_size = 2 + FREE_BLOCK_SLOTS * 2;
    

        

        
    // }


    
    /// Solves an allocation, this means we update the information used
    /// to make the allocation after we are done.
    fn solve_allocate(&mut self, allocation: Allocation) -> Result<(), PageError> {
        if allocation.is_lead_allocation {
            // Simple to solve these allocations, we just push the pointer. 
            self.set_lead_offset(self.get_lead_offset() + allocation.size);
            
        } else {
            // A little more complex.
            let Some(fb) = allocation.free_block else {
                // If this is none then that means we did not
                // allocate through the free pointer NOR did we allocate
                // through the lead pointer which makes no sense.
                return Err(PageError::BadAllocation);
            };
            if allocation.size == fb.size as usize {
                // We have used it up, so this is empty.
                FreeBlock::write(self, fb.position, fb.next as usize, 0, 0);
            } else {
                println!("Allocation size: {}, Total: {}", allocation.size, fb.size);
                // We have not used this up, so it is not empty.
                self.adjust_free_block(fb.offset as usize + allocation.size, fb.size as usize - allocation.size, fb);
                // FreeBlock::write(self, fb.position, fb.next as usize, , fb.size as usize - allocation.size);
            }
            
        }
        self.set_used_space(self.get_used_space() + allocation.size);
        Ok(())
    }

    fn adjust_free_block(&mut self, new_offset: usize, new_size: usize, block: FreeBlock) {
        if new_size <= FRAGMENTATION_SIZE {
            // No point, these are just fragmented bytes.
            FreeBlock::write(self, block.position, block.next as usize, 0, 0);
            self.set_fragmented(self.get_fragmented() + new_size);
        } else {
            // Adjust the free block details.
            FreeBlock::write(self, block.position, block.next as usize, new_offset, new_size);
        }
    }


    /// Updates the free list in the page.
    fn update_free_list(&mut self, start: usize, size: usize) -> Result<(), PageError> {
        // if size <= FRAGMENTATION_SIZE {
        //     // No real point in doing anything with this.
        //     self.set_fragmented(self.get_fragmented() + size);
        //     return Ok(())
        // }
        let free_list_pointer = self.get_free_block_ptr();
        if free_list_pointer == 0 {
            // The free chain thing is not initialized yet.
            let new_ptr = self.allocate_free_block(start, size)?;
            self.set_free_ptr(new_ptr);
        } else {
            // We do have a free chain.
            println!("Descending down chain");
            self.update_free_chain(None, free_list_pointer, start, size)?;
        }

        Ok(())
    }

    /// Tries to allocate a freelist block at a pointer.
    fn allocate_free_block(&mut self, offset: usize, size: usize) -> Result<usize, PageError> {
        // Define and determine a location.
        let allocate = self.can_allocate(FreeBlock::size()).ok_or_else(|| PageError::LeafPageFull)?;
        let location = allocate.location;
        // Write the new free block.
        FreeBlock::write(self, allocate.location, 0, offset, size);
        // Solve the allocatin.
        self.solve_allocate(allocate)?;

        // increase the fragmented size
        self.set_fragmented(self.get_fragmented() + FreeBlock::size());
        Ok(location)
    }
    /// Find a canddate for upating the chain
    fn update_free_chain(&mut self, previous: Option<FreeBlock>, pointer: usize, start: usize, size: usize) -> Result<(), PageError> {
        let current = FreeBlock::read(pointer, &*self)?;
        let next_ptr = current.next as usize;
        if current.offset == 0 {
            // Use a non-active block.
            FreeBlock::write(self, pointer, next_ptr, start, size);
            Ok(())
        }
        else if (start + size) == current.offset as usize {
            // println!("LEFT EDGE...");
            // exit(1);
            // Solves a left edge.
            //
            // Essentially, if there are two open spaces next to eachoher it is more efficient to combine them.
            FreeBlock::write(self, current.position, current.next as usize, start, current.size as usize + size);
            Ok(())
        } else if (current.offset + current.size) as usize == start {
            // Solves a right edge.
            //
            // Essentially, if there are two open spaces right next to eachother, it is more efficient to consider them a single
            // open space.
            // We just extend this block.
            FreeBlock::write(self, current.position, current.next as usize, current.offset as usize, current.size as usize + size);
            // println!("SOLVED EGDE");
            Ok(())
        } else if current.next == 0 {
            println!("Descending allocating new blocck w/ {}", size);
            // End of the chain, allocate a new block
            
            let new_block = self.allocate_free_block(start, size)?;
            println!("New pointer for {:?} is {new_block}", previous);
            
            // Update the previous block.
            FreeBlock::write(self, current.position, new_block, current.offset as usize, current.size as usize);
        
            
            Ok(())
        } else {
            println!("Descending with size {}", size);
            // Continue down the chain recursively.
            self.update_free_chain(Some(current), next_ptr as usize, start, size)
        }
        // let block = FreeBlock::read(previous.next, self)?;
    }
    
    /// Deletes a record from the database.
    /// 
    /// This will not perform any sort of rebalancing on the page.
    /// It instead will just delete the record.
    /// 
    /// It will however shift over the offsets.
    fn simple_delete(&mut self, record: usize) -> Result<(), PageError> {
        
        if !self.check_record_exists(record) {
            // println!("DELETING RECORD E: {record}");
            // if record == 2 { exit(1) };
            return Err(PageError::NoRecordFound)?;
        }
        

        

        

        // The initial cell count before we perform the deletion.
        let initial_cell_count = self.get_cell_count();

        // Update the size.
        let size = self.get_total_record_size(record).unwrap();
        let offset = self.get_offset(record);

        // Delete the actual offset.
        let index_ptr = 2 * record + Projection::<Leaf>::header_size();
        self[index_ptr..index_ptr + 2].fill(0);

        // Delete the actual data.
        self[offset..offset + size].fill(0);

        // Update the used space.
        self.set_used_space(self.get_used_space() - (2 + size));

        // Update the record count.
        self.set_cell_count(initial_cell_count - 1);

        // Shift the offsets over.
        if initial_cell_count > 1 {
            // If we had more than one we are going to need to shift these over.
            let to_shift = Projection::<Leaf>::calculate_offset_index(record);
            let end_shft = Projection::<Leaf>::calculate_offset_index(initial_cell_count - 1);

            // Recall that the record offset is zeroed,
            // so we just need to rotate the
            // array left and we are good!
            self[to_shift..end_shft + 2].rotate_left(2);
        }
        
        // Update the free list. ONLY if the size is
        println!("Making call to UFL {}", size);
        self.update_free_list(offset, size)?;
        

        Ok(())
    }
}




#[cfg(test)]
mod tests {
    use std::error::Error;

    use overseer::{error::NetworkError, models::Value};
    use tempfile::tempdir;

    use crate::database::store::{file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE}, paging::{error::PageError, leaf_page::{FreeBlock, Record}, page::{Projection, Transact}}};

    use super::Leaf;

  

    // TODO: Add unit tests to test mid-write failure.




    #[monoio::test]
    pub async fn test_leaf_page_fill_fragmented() -> Result<(), Box<dyn Error + 'static>> {
        let dir = tempdir()?;
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await?;
        paged.new_page().await?.leaf().open(&paged, async |leaf: &mut Transact<Leaf>| {
            
            // Get the records.
            let record_a = Record { value: Some(Value::Integer(339939393)) }.produce().await;
            let record_b = Record { value: Some(Value::Integer(332)) }.produce().await;
            let record_c = Record { value: Some(Value::Integer(83920320039092)) }.produce().await;
            let record_d = Record { value: Some(Value::Integer(83920320039092333)) }.produce().await;

            // Get the totals.
            let total_a = record_a.total_serialized_size();
            let total_b = record_b.total_serialized_size();
            let total_c = record_c.total_serialized_size();
            let total_d = record_d.total_serialized_size();

            // Write the records.
            leaf.write_serialized_record(record_a).await?;
            leaf.write_serialized_record(record_b).await?;
            leaf.write_serialized_record(record_c).await?;
            
     

            // Delete.
            leaf.simple_delete(1)?;

            // Verify the free chain is correct.
            let fc = leaf.read_free_chain()?;
            assert_eq!(fc.len(), 1);
            assert_eq!(fc.first().unwrap().size as usize, total_b);

            leaf.simple_delete(0)?;

            // Verify the free chain is correct.
            let fc = leaf.read_free_chain()?;
            assert_eq!(fc.len(), 1);
            assert_eq!(fc.first().unwrap().size as usize, total_b + total_a);

        
            leaf.simple_delete(0)?;

            // Verify the free chain is correct.
            let fc = leaf.read_free_chain()?;
            assert_eq!(fc.len(), 1);
            assert_eq!(fc.first().unwrap().size as usize, total_a + total_b + total_c);

            // This allocation should be performed into fragmented space.
            leaf.write_serialized_record(record_d).await?;

            // Verify the free chain is correct.
            let fc = leaf.read_free_chain()?;
            assert_eq!(fc.len(), 1);
            assert_eq!(fc.first().unwrap().size as usize, (total_a + total_b + total_c) - total_d);

            // println!("leaf: {:?}", &leaf[..40]);
            // println!("leaf: {:?}", &leaf[4020..]);
            // println!("wow: {:?}", leaf.read_free_chain().unwrap());

            // leaf.write_record(Record { value: Some(Value::Integer(3)) }).await?;

            // println!("wow: {:?}", leaf.read_free_chain().unwrap());
            // println!("leaf: {:?}", &leaf[..40]);
            // println!("leaf: {:?}", &leaf[4020..]);
            

            Ok(())
        }).await?;

        Ok(())
    }


    /// This just deletes a single record from the database.
    #[monoio::test]
    pub async fn test_leaf_page_deletion_basic() -> Result<(), Box<dyn Error + 'static>> {
        let dir = tempdir()?;
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await?;
        paged.new_page().await?.leaf().open(&paged, async |leaf: &mut Transact<Leaf>| {
            
            leaf.write_record(Record { value: Some(Value::Integer(339939393)) }).await?;
            leaf.write_record(Record { value: Some(Value::Integer(332)) }).await?;
            leaf.write_record(Record { value: Some(Value::Integer(83920320039092)) }).await?;
            
            // println!("hello: {:?}", leaf.view());

        
            leaf.simple_delete(1)?;

            
            assert_eq!(leaf.get_cell_count(), 2);
            assert_eq!(leaf.read_record(0).await?.unwrap().value, Some(Value::Integer(339939393)));
            assert_eq!(leaf.read_record(1).await?.unwrap().value, Some(Value::Integer(83920320039092)));

    


        

            Ok(())
        }).await?;

        Ok(())
    }


    #[monoio::test]
    pub async fn test_leaf_page_basic_set_get_cell_count() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let leaf = paged.new_page().await.unwrap().leaf();
        assert_eq!(leaf.get_cell_count(), 0);

        let leaf = leaf.open(&paged, async |leaf| {
            leaf.set_cell_count(24);

            Ok(())
        }).await.unwrap();
   
    

        // leaf.set_cell_count(&paged, 24).await.unwrap();

        assert_eq!(leaf.get_cell_count(), 24);
    }

    #[monoio::test]
    pub async fn test_leaf_space_calculaion() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        paged.new_page().await.unwrap().leaf().open(&paged, async |leaf: &mut Transact<Leaf>| {

            let base = PAGE_SIZE as usize - PAGE_HEADER_RESERVED_BYTES as usize - Projection::<Leaf>::header_size();
            assert_eq!(leaf.get_free_space(), base);
            let record = Record { value: Some(Value::Integer(32)) }.produce().await;
            let record_space = record.total_serialized_size();
            leaf.write_serialized_record(record).await?;
            assert_eq!(leaf.get_free_space(), base - record_space - 2);
            leaf.simple_delete(0)?;
            assert_eq!(leaf.get_free_space(), base - FreeBlock::size());
            Ok(())
        }).await.unwrap();
        

        
        
    }

    #[monoio::test]
    pub async fn test_leaf_page_write_overfit() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();

        let massive_string = String::from_utf8(vec![23u8; PAGE_SIZE + 2]).unwrap();

        paged.new_page().await.unwrap().leaf().open(&paged, async |leaf| {
            
            let result: Result<(), PageError> = leaf.write_record(Record { value: Some(Value::String(massive_string)) }).await;
            if let Err(result) = result {
                
                assert_eq!(result.variant(), PageError::LeafPageFull.variant());
            } else {
                panic!("Should have errored on overflow but did not.");
            }
            

            Ok(())
        }).await.unwrap();
        // let leaf = LeafPage::new(paged.new_page().await.unwrap());
        

        // panic!("LEAF: {:?}", leaf.inner.hexdump(&paged).await.unwrap());

        
    }

    #[monoio::test]
    pub async fn test_leaf_page_write_record() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();

        paged.new_page().await.unwrap().leaf().open(&paged, async |leaf| {
            leaf.write_record(Record { value: Some(Value::Integer(32)) }).await.unwrap();
            leaf.write_record(Record { value: Some(Value::Integer(21)) }).await.unwrap();
            leaf.write_record(Record { value: Some(Value::String("hello andrew".to_string())) }).await.unwrap();

            assert_eq!(leaf.get_cell_count(), 3);



            assert_eq!(leaf.read_record(0).await.unwrap().unwrap().value, Some(Value::Integer(32)));
            assert_eq!(leaf.read_record(1).await.unwrap().unwrap().value, Some(Value::Integer(21)));
            assert_eq!(leaf.read_record(2).await.unwrap().unwrap().value, Some(Value::String("hello andrew".to_string())));

            Ok(())
        }).await.unwrap();
        // let leaf = LeafPage::new(paged.new_page().await.unwrap());
        

        // panic!("LEAF: {:?}", leaf.inner.hexdump(&paged).await.unwrap());

        
    }
}