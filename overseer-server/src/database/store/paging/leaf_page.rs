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
    pub fn get_cell_count(&self) -> u16 {
        u16::from_le_bytes(self[0..2].try_into().unwrap())
    }
    

    pub fn get_offset(&self, index: u32) -> u16 {
        let pos = (2 + index * 2) as usize;
        u16::from_le_bytes(self[pos .. pos + 2].try_into().unwrap())
    }

    pub fn get_remaining_space(&self) -> usize {
        let count = self.get_cell_count();
        if count == 0 {
            self.capacity() - 2
        } else {
            // The count + Offsets + the last offset.
            let total_used = 2 + 2 * count as usize;
            let final_offset = self.size() - self.get_offset((count - 1) as u32) as usize - PAGE_HEADER_RESERVED_BYTES as usize;
            self.capacity() - (total_used + final_offset)
        }
    }
    /// Checks if a record will fit into the database.
    pub fn will_fit(&self, record: &SerializedRecord) -> bool {
        let remaining = self.get_remaining_space();
        record.data.len() + 2 <= remaining as usize
    }
    
    pub async fn read_record(&self, index: u16) -> Result<Option<Record>, NetworkError>
    {
        if index >= self.get_cell_count() {
            return Ok(None);
        } else {
            // Calculate the record offset.
            let offset = self.get_offset(index as u32);

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
    fn find_new_record_ptr(&self, cell_count: u16, record: &SerializedRecord) -> u32 {
        let record_ptr;
        if cell_count == 1 {
            // first record in the page.
            record_ptr = self.capacity() - record.data.len();

        } else {
            // Get the previous record offset.
            let record_offset = self.get_offset((cell_count - 2) as u32);

            record_ptr = record_offset as usize - record.data.len();
        }
        record_ptr as u32
    }
    
    
}

impl Transact<Leaf> {
    /// Increments and returns the new Cell count.
    pub fn increment_cell_count(&mut self) -> u16 {
        let new = self.get_cell_count() + 1;
        self.set_cell_count(new);
        new
    }
    pub fn set_cell_count(&mut self, cells: u16) {
        self[0..2].copy_from_slice(&cells.to_le_bytes());
        // self.inner.write(file, 0, cells.to_le_bytes().to_vec()).await?;
 
    }
    /// Writes a new offset given the offset index and the
    /// actual record ptr.
    fn write_record_offset(&mut self, offset_index: u16, record_ptr: u32) {
        let offset = (2 + offset_index * 2) as usize;
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
        self.write_record_offset(new_cell_count - 1, record_ptr as u32);
        
        Ok(())
        
    }
}


#[cfg(test)]
mod tests {
    use overseer::{error::NetworkError, models::Value};
    use tempfile::tempdir;

    use crate::database::store::{file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE}, paging::leaf_page::Record};

  

    // TODO: Add unit tests to test mid-write failure.


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