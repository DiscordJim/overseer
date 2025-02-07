use overseer::models::{Key, Value};


/// The storage driver for the database. Without this we cannot store things.
pub struct DatabaseDriver {

}

impl DatabaseDriver {

    pub fn store(&self, key: &Key, value: &Value) {
        
    }
}