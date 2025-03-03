use overseer::{error::NetworkError, models::Value};

use crate::database::store::file::{Page, PagedFile, PAGE_SIZE};


/// The format of the leaf page starts with a cell count (2-byte)
pub struct LeafPage {
    inner: Page
}

pub struct Record {
    value: Option<Value>
}


impl Record {
    pub fn to_bytes(&self) -> Vec<u8> {
        [1,2,3].to_vec()
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


    // pub async fn read_record(&self, index: u32, file: &PagedFile) -> Result<u16, NetworkError> {

    // }
    

    pub async fn write_record(&self, record: Record, file: &PagedFile) -> Result<(), NetworkError> {
        let new_cell_count = self.get_cell_count(file).await? + 1;
        // Increments the cell count.
        self.set_cell_count(file, new_cell_count).await?;

        


        let record_bytes = record.to_bytes().to_vec();

        let record_ptr;
        if new_cell_count == 1 {
            // first record in the page.
            record_ptr = self.inner.capacity() - record_bytes.len() as u32;

        

            

        } else {
            // Get the previous record offset.
            let record = self.get_offset((new_cell_count - 2) as u32, file).await?;

            record_ptr = record as u32 - record_bytes.len() as u32;
        }

        // Write the record.
        self.inner.write(file, record_ptr, record_bytes).await?;

        // Calculate the offset.
        let offset = 2 + (new_cell_count - 1) * 2;
        self.inner.write(file, offset as u32, (record_ptr as u16).to_le_bytes().to_vec()).await?;



        

        Ok(())
        
    }
    
}


#[cfg(test)]
mod tests {
    use overseer::models::Value;
    use tempfile::tempdir;

    use crate::database::store::{file::PagedFile, paging::page::Record};

    use super::LeafPage;


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
    pub async fn test_leaf_page_write_record() {
        let dir = tempdir().unwrap();
        let mut paged = PagedFile::open(dir.path().join("hello.txt")).await.unwrap();
        let leaf = LeafPage::new(paged.new_page().await.unwrap());
        leaf.write_record(Record { value: Some(Value::Integer(32)) }, &paged).await.unwrap();
        leaf.write_record(Record { value: Some(Value::Integer(21)) }, &paged).await.unwrap();

        assert_eq!(leaf.get_cell_count(&paged).await.unwrap(), 2);
    }
}