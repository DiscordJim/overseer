use std::io::Cursor;

use overseer::{error::NetworkError, models::Value, network::OverseerSerde};


use crate::database::store::file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE};

use super::page::{Page, Projection, Transact};


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
    data: Vec<u8>
}

impl Record {
    pub async fn produce(self) -> Result<SerializedRecord, NetworkError> {
        let mut cursor = Cursor::new(vec![]);
        self.serialize(&mut cursor).await.unwrap();
        Ok(SerializedRecord {
            value: self,
            data: cursor.into_inner()
        })
    }
}

impl SerializedRecord {
    pub fn to_record(self) -> Record {
        self.value
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

impl Projection<Leaf> {
    pub fn get_cell_count(&self) -> usize {
        u16::from_le_bytes(self[0..2].try_into().unwrap()) as usize
    }
    

    pub fn get_offset(&self, index: usize) -> usize {
        let pos = (2 + index * 2) as usize;
        u16::from_le_bytes(self[pos .. pos + 2].try_into().unwrap()) as usize
    }

    pub fn get_remaining_space(&self) -> usize {
        let count = self.get_cell_count();
        if count == 0 {
            self.capacity() - 2
        } else {
            // The count + Offsets + the last offset.
            let total_used = 2 + 2 * count as usize;
            let final_offset = self.size() - self.get_offset((count - 1) as usize) as usize - PAGE_HEADER_RESERVED_BYTES as usize;
            self.capacity() - (total_used + final_offset)
        }
    }
    /// Checks if a record will fit into the database.
    pub fn will_fit(&self, record: &SerializedRecord) -> bool {
        let remaining = self.get_remaining_space();
        record.data.len() + 2 <= remaining as usize
    }

    /// Determines the size of a record on the leaf page.
    pub fn get_record_size(&self, record: usize) -> Option<usize> {
        if record >= self.get_cell_count() as usize {
            None 
        } else if record == 0 {
            Some(self.capacity() - self.get_offset(record) as usize)
        } else {
            Some(self.get_offset(record - 1) as usize - self.get_offset(record) as usize)
        }
    }
    
    pub async fn read_record(&self, index: usize) -> Result<Option<Record>, NetworkError>
    {
        if index >= self.get_cell_count() {
            return Ok(None);
        } else {
            // Calculate the record offset.
            let offset = self.get_offset(index);

            let mut reader = self.reader(offset as usize);
            let record = Record::deserialize(&mut reader).await?;

            // let offset = self.get_offset(index as u32) + PAGE_HEADER_RESERVED_BYTES as u16;
            // let mut page_reader = paged.reader(actual_offset);
            // let record = Record::deserialize(&mut page_reader).await?;

            Ok(Some(record))
        }
    }


    

    
    /// Finds a new record pointer location in the file. This takes in the new cell count,
    /// which is the cell count including this new record.
    fn find_new_record_ptr(&self, cell_count: usize, record: &SerializedRecord) -> usize {
        let record_ptr;
        if cell_count == 1 {
            // first record in the page.
            record_ptr = self.capacity() - record.data.len();

        } else {
            // Get the previous record offset.
            let record_offset = self.get_offset((cell_count - 2) as usize);

            record_ptr = record_offset as usize - record.data.len();
        }
        record_ptr
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
    /// Writes a new offset given the offset index and the
    /// actual record ptr.
    fn write_record_offset(&mut self, offset_index: usize, record_ptr: usize) {
        let offset = 2 + offset_index * 2;
        self[offset..offset + 2].copy_from_slice(&(record_ptr as u16).to_le_bytes());
        // self.inner.write(file, offset as u32, (record_ptr as u16).to_le_bytes().to_vec()).await?;
        // Ok(())
    }
    pub async fn write_record(&mut self, record: Record) -> Result<(), NetworkError> {
        self.write_serialized_record(record.produce().await?).await
    }
    pub async fn write_serialized_record(&mut self, record: SerializedRecord) -> Result<(), NetworkError> {
        if !self.will_fit(&record) {
            return Err(NetworkError::RecordWontFit);
        }
        // Increment and get the new cell count.
        let new_cell_count = self.increment_cell_count();

        // Find a new record pointer.
        let record_ptr = self.find_new_record_ptr(new_cell_count, &record) as usize;
        

        // Write the record.
        self[record_ptr..record_ptr + record.data.len()].copy_from_slice(&record.data);
        // self.inner.write(file, record_ptr, record.data).await?;

        // Calculate the offset.
        self.write_record_offset(new_cell_count as usize - 1, record_ptr);
        
        Ok(())
        
    }
    pub async fn delete_record(&mut self, record: usize) -> Result<(), NetworkError> {
        if record >= self.get_cell_count() as usize {
            // This does not exist.
            return Err(NetworkError::PageOutOfBounds)?;
        } else if self.get_cell_count() == 1 {
            self.set_cell_count(0);
            let start=  self.get_offset(record);
            let size = self.get_record_size(record).unwrap();
            self[start..start + size].fill(0);
            self.write_record_offset(0, 0);
            return Ok(())
        }

        // Get the record size, this will help us calculate the offsets.
        let record_size = self.get_record_size(record).unwrap();
        let record_offset = self.get_offset(record);
        println!("Deleting a record of size {}", record_size);

        // println!("View (0): {:?}", &self.view()[..40]);

        // Move the other stuff on the record.
        let lower = self.get_offset(self.get_cell_count() - 1);
        let upper = self.get_offset(record + 1) + self.get_record_size(record + 1).unwrap();
        println!("View (1): {:?}", &self.view()[4050..]);

        let to_move = self[lower..upper].to_vec();
        println!("To move: {:?}", to_move);

        self[lower..upper + record_size].fill(0);
        println!("View (2): {:?}", &self.view()[4050..]);

        // Actually shift the bytes.
        self[lower + record_size..upper + record_size].copy_from_slice(&to_move);
        println!("View (3): {:?}", &self.view()[4050..]);



        for i in record + 1..self.get_cell_count() as usize {
            // println!("Shifing {i}, current: {}, new: {}", self.get_offset(i as u32) as usize + record_size);
            // Shift over the offset.
            self.write_record_offset(i, self.get_offset(i) + record_size);

        }



        

        // Shift over the offsets.
        // println!("View (1): {:?}", &self.view()[..40]);
        
        let old = 2 * (record + 1) + 2;
        let old_upper = 2 * (self.get_cell_count()) + 2;
        let to_move = &self[old..old_upper].to_vec();
        println!("TOP (0): {:?}", &self.view()[..40]);
        println!("To move: {:?}", to_move);
        self[old - 2..old_upper - 2].copy_from_slice(to_move);
        self[old_upper - 2..old_upper].fill(0);
        self.set_cell_count(self.get_cell_count() - 1);


        
        println!("TOP: {:?}", &self.view()[..40]);
        // self[lower..]

        // Shift over he acual rcord.

        // println!("View (2): {:?}", &self.view()[..40]);
        



        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::error::Error;

    use overseer::{error::NetworkError, models::Value};
    use tempfile::tempdir;

    use crate::database::store::{file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE}, paging::{leaf_page::Record, page::Transact}};

    use super::Leaf;

  

    // TODO: Add unit tests to test mid-write failure.

    #[monoio::test]
    pub async fn test_leaf_page_deletion() -> Result<(), Box<dyn Error + 'static>> {
        let dir = tempdir()?;
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await?;
        let page = paged.new_page().await?.leaf().open(&paged, async |leaf: &mut Transact<Leaf>| {
            
            leaf.write_record(Record { value: Some(Value::Integer(339939393)) }).await?;
            leaf.write_record(Record { value: Some(Value::Integer(332)) }).await?;
            leaf.write_record(Record { value: Some(Value::Integer(83920320039092)) }).await?;
            
            // println!("hello: {:?}", leaf.view());

            println!("Hello: {:?}", leaf.get_record_size(1));

            leaf.delete_record(0).await?;
            
            assert_eq!(leaf.get_cell_count(), 2);
            assert_eq!(leaf.read_record(0).await?.unwrap().value, Some(Value::Integer(332)));
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
        paged.new_page().await.unwrap().leaf().open(&paged, async |leaf| {
            assert_eq!(leaf.get_remaining_space(), PAGE_SIZE as usize - PAGE_HEADER_RESERVED_BYTES as usize - 2);
            leaf.write_record(Record { value: Some(Value::Integer(32)) }).await?;
            assert_eq!(leaf.get_remaining_space(), PAGE_SIZE as usize - PAGE_HEADER_RESERVED_BYTES as usize - 4 - 3);
            Ok(())
        }).await.unwrap();
        

        
        
    }

    #[monoio::test]
    pub async fn test_leaf_page_write_overfit() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();

        let massive_string = String::from_utf8(vec![23u8; PAGE_SIZE + 2]).unwrap();

        paged.new_page().await.unwrap().leaf().open(&paged, async |leaf| {
            
            let result = leaf.write_record(Record { value: Some(Value::String(massive_string)) }).await;
            if let Err(result) = result {
                assert_eq!(result.to_string(), NetworkError::RecordWontFit.to_string());
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