use std::io::Cursor;

use overseer::{error::NetworkError, models::Value, network::OverseerSerde};


use crate::database::store::file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE};

use super::page::Page;


/// The format of the leaf page starts with a cell count (2-byte)
pub struct LeafPage {
    inner: Page
}

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

impl LeafPage {
    pub fn new(inner: Page) -> Self {
        Self {
            inner
        }
    }
   
    pub async fn get_cell_count(&self, file: &PagedFile) -> Result<u16, NetworkError> {
        let val: [u8; 2] = self.inner.read(file, 0, 2).await?.try_into().unwrap();
        Ok(u16::from_le_bytes(val))
    }
    pub async fn set_cell_count(&self, file: &PagedFile, cells: u16) -> Result<(), NetworkError> {
        self.inner.write(file, 0, cells.to_le_bytes().to_vec()).await?;
        Ok(())
    }

    pub async fn get_offset(&self, index: u32, file: &PagedFile) -> Result<u16, NetworkError> {
        let val: [u8; 2] = self.inner.read(file, 2 + index * 2, 2).await?.try_into().unwrap();
        Ok(u16::from_le_bytes(val))
    }

    pub async fn get_remaining_space(&self, paged: &PagedFile) -> Result<u32, NetworkError> {
        let count = self.get_cell_count(paged).await?;
        if count == 0 {
            Ok(self.inner.capacity() - 2)
        } else {
            // The count + Offsets + the last offset.
            let total_used = 2 + 2 * count as u32;
            let final_offset = self.inner.size() - self.get_offset((count - 1) as u32, paged).await? as u32 - PAGE_HEADER_RESERVED_BYTES;
            Ok(self.inner.capacity() - (total_used + final_offset))
        }
    }
    /// Checks if a record will fit into the database.
    pub async fn will_fit(&self, record: &SerializedRecord, paged: &PagedFile) -> Result<bool, NetworkError> {
        let remaining = self.get_remaining_space(paged).await?;
        Ok(record.data.len() + 2 <= remaining as usize)
    }
    
    pub async fn read_record(&self, index: u16, paged: &PagedFile) -> Result<Option<Record>, NetworkError>
    {
        if index >= self.get_cell_count(&paged).await? {
            return Ok(None);
        } else {
            let offset = self.get_offset(index as u32, paged).await?;
            let actual_offset = self.inner.get_write_ptr(offset as u32).as_u64() as usize;
            let mut page_reader = paged.reader(actual_offset);
            let record = Record::deserialize(&mut page_reader).await?;
            // let record = Record::deserialize();
            Ok(Some(record))
        }
    }


    pub async fn write_record(&self, record: Record, file: &PagedFile) -> Result<(), NetworkError> {
        self.write_serialized_record(record.produce().await?, file).await
    }
    /// Increments and returns the new Cell count.
    async fn increment_cell_count(&self, file: &PagedFile) -> Result<u16, NetworkError> {
        let new = self.get_cell_count(file).await? + 1;
        self.set_cell_count(file, new).await?;
        Ok(new)
    }
    /// Finds a new record pointer location in the file. This takes in the new cell count,
    /// which is the cell count including this new record.
    async fn find_new_record_ptr(&self, cell_count: u16, record: &SerializedRecord, file: &PagedFile) -> Result<u32, NetworkError> {
        let record_ptr;
        if cell_count == 1 {
            // first record in the page.
            record_ptr = self.inner.capacity() - record.data.len() as u32;

        } else {
            // Get the previous record offset.
            let record_offset = self.get_offset((cell_count - 2) as u32, file).await?;

            record_ptr = record_offset as u32 - record.data.len() as u32;
        }
        Ok(record_ptr)
    }
    /// Writes a new offset given the offset index and the
    /// actual record ptr.
    async fn write_record_offset(&self, offset_index: u16, record_ptr: u32, file: &PagedFile) -> Result<(), NetworkError> {
        let offset = 2 + offset_index * 2;
        self.inner.write(file, offset as u32, (record_ptr as u16).to_le_bytes().to_vec()).await?;
        Ok(())
    }
    pub async fn write_serialized_record(&self, record: SerializedRecord, file: &PagedFile) -> Result<(), NetworkError> {
        // Increment and get the new cell count.
        let new_cell_count = self.increment_cell_count(file).await?;

        // Find a new record pointer.
        let record_ptr = self.find_new_record_ptr(new_cell_count, &record, file).await?;
        

        // Write the record.
        self.inner.write(file, record_ptr, record.data).await?;

        // Calculate the offset.
        self.write_record_offset(new_cell_count - 1, record_ptr, file).await?;
        
        Ok(())
        
    }
    
}



#[cfg(test)]
mod tests {
    use overseer::models::Value;
    use tempfile::tempdir;

    use crate::database::store::{file::{PagedFile, PAGE_HEADER_RESERVED_BYTES, PAGE_SIZE}, paging::leaf_page::Record};

    use super::LeafPage;

    // TODO: Add unit tests to test mid-write failure.


    #[monoio::test]
    pub async fn test_leaf_page_basic_set_get_cell_count() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let leaf = LeafPage::new(paged.new_page().await.unwrap());
        assert_eq!(leaf.get_cell_count(&paged).await.unwrap(), 0);

        leaf.set_cell_count(&paged, 24).await.unwrap();

        assert_eq!(leaf.get_cell_count(&paged).await.unwrap(), 24);
    }

    #[monoio::test]
    pub async fn test_leaf_space_calculaion() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let leaf = LeafPage::new(paged.new_page().await.unwrap());
        assert_eq!(leaf.get_remaining_space(&paged).await.unwrap(), PAGE_SIZE as u32 - PAGE_HEADER_RESERVED_BYTES - 2);

        leaf.write_record(Record { value: Some(Value::Integer(32)) }, &paged).await.unwrap();
        assert_eq!(leaf.get_remaining_space(&paged).await.unwrap(), PAGE_SIZE as u32 - PAGE_HEADER_RESERVED_BYTES - 4 - 3);
    }

    #[monoio::test]
    pub async fn test_leaf_page_write_record() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let leaf = LeafPage::new(paged.new_page().await.unwrap());
        leaf.write_record(Record { value: Some(Value::Integer(32)) }, &paged).await.unwrap();
        leaf.write_record(Record { value: Some(Value::Integer(21)) }, &paged).await.unwrap();
        leaf.write_record(Record { value: Some(Value::String("hello andrew".to_string())) }, &paged).await.unwrap();

        // panic!("LEAF: {:?}", leaf.inner.hexdump(&paged).await.unwrap());

        assert_eq!(leaf.get_cell_count(&paged).await.unwrap(), 3);

        assert_eq!(leaf.read_record(0, &paged).await.unwrap().unwrap().value, Some(Value::Integer(32)));
        assert_eq!(leaf.read_record(1, &paged).await.unwrap().unwrap().value, Some(Value::Integer(21)));
        assert_eq!(leaf.read_record(2, &paged).await.unwrap().unwrap().value, Some(Value::String("hello andrew".to_string())));
    }
}