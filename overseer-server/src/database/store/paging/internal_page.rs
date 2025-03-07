use super::page::{Projection, Transact};



pub struct Internal;

impl Projection<Internal> {
    
}

impl Transact<Internal> {

}


#[cfg(test)]
mod tests {
    use std::error::Error;


    #[monoio::test]
    pub async fn test_internal_page_basic() -> Result<(), Box<dyn Error>> {


        Ok(())
    }
 
}