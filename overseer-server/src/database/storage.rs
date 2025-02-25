use std::{collections::HashMap, path::{Path, PathBuf}, sync::RwLock};

use overseer::{error::NetworkError, models::{Key, Value}};


/// The storage driver for the database. Without this we cannot store things.
pub struct DatabaseStorage
{
    location: PathBuf,
    hashmap: RwLock<HashMap<Key, Value>>
    // pool: Pool<Sqlite>
}

pub struct StoredRecord {
    pub key: Key,
    pub value: Value
}

impl DatabaseStorage {
    pub async fn new<P, S>(path: P, name: S) -> Result<Self, NetworkError>
    where 
        P: AsRef<Path>,
        S: AsRef<str>
    {

        let path = path.as_ref().join(name.as_ref());
 
        let inner = if path.exists() {
            bincode::deserialize(&monoio::fs::read(&path).await?).unwrap()
        } else {
            HashMap::new()
        };
        
        
        Ok(Self {
            location: path,
            hashmap: RwLock::new(inner)
        })
    }
    pub async fn write(&self, key: &Key, value: &Value) -> Result<(), NetworkError> {
        self.hashmap.write().unwrap().insert(key.clone(), value.to_owned());
        // sqlx::query("INSERT INTO kv_table(key, type, data) VALUES ($1, $2, $3)")
        //     .bind(key.as_str())
        //     .bind(value.discriminator())
        //     .bind(value.as_bytes())
        //     .execute(&self.pool)
        //     .await?;
        self.save().await?;
        Ok(())
    }
    async fn save(&self) -> Result<(), NetworkError> {
        let s= bincode::serialize(&*self.hashmap.read().unwrap()).unwrap();
        let (r, a) = monoio::fs::write(&self.location, s).await;
        r?;
        Ok(())
    }
    
    pub async fn update(&self, key: &Key, value: Value) -> Result<(), NetworkError> {
        self.write(key, &value).await?;
        // sqlx::query("UPDATE kv_table SET type = $1, data = $2  WHERE key = $3")
        //     .bind(value.discriminator())
        //     .bind(value.as_bytes())
        //     .bind(key.as_str())
        //     .execute(&self.pool).await?;

        Ok(())

    }
    pub async fn delete(&self, key: &Key) -> Result<(), NetworkError> {
        // sqlx::query("DELETE FROM kv_table WHERE key = $1")
        //     .bind(key.as_str())
        //     .execute(&self.pool)
        //     .await?;
        self.hashmap.write().unwrap().remove(key);
        self.save().await?;
        Ok(())
    }
    pub async fn records(&self) -> Vec<(Key, Value)> {
        self.hashmap.read().unwrap().iter().map(|f| (f.0.clone(), f.1.clone())).collect()
    }
    // pub async fn read(&self) -> Result<Vec<StoredRecord>, NetworkError> {
    //     let rows = sqlx::query("SELECT * FROM kv_table;")
    //         .fetch_all(&self.pool).await?
    //         .into_iter().map(read_sqliterow)
    //         .collect::<Result<Vec<_>, NetworkError>>()?;
    //     Ok(rows)
    // }
}


// fn read_sqliterow(row: SqliteRow) -> Result<StoredRecord, NetworkError> {
//     let r = row.get::<String, _>(1);
//     let v_type = row.get::<i64, _>(2);
//     let bytes = row.get::<Vec<u8>, _>(3);

//     Ok(StoredRecord {
//         key: r.into(),
//         value: Value::decode(v_type as u8, &bytes)?
//     })
// }




// async fn setup_table(pool: &Pool<Sqlite>) -> Result<(), NetworkError>
// {

//     sqlx::query("CREATE TABLE IF NOT EXISTS kv_table (
//     id INTEGER PRIMARY KEY AUTOINCREMENT,
//     key TEXT,
//     type INTEGER,
//     data BLOB
//     );").execute(pool).await?;

//     Ok(())
// }

#[cfg(test)]
mod tests {

    use overseer::models::{Key, Value};

    use crate::database::DatabaseStorage;


    // #[tokio::test]
    // pub async fn test_db_rw_record() {
    //     let tf = tempfile::tempdir().unwrap();
    //     let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
    //     da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

    //     let records = da.records().await;
    //     assert_eq!(records.len(), 1);
    //     assert_eq!(records.first().unwrap().1, Value::Integer(21));
    // }

    // #[tokio::test]
    // pub async fn test_db_update_record() {
    //     let tf = tempfile::tempdir().unwrap();
    //     let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
    //     da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

    //     da.update(&Key::from_str("hello"), Value::Integer(23)).await.unwrap();

    //     let records =da.records().await;
    //     assert_eq!(records.len(), 1);
    //     assert_eq!(records.first().unwrap().1, Value::Integer(23));
    // }

    // #[tokio::test]
    // pub async fn test_db_rw_record_delete() {
    //     let tf = tempfile::tempdir().unwrap();
    //     let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
    //     da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

    //     da.delete(&Key::from_str("hello")).await.unwrap();

    //     let records = da.records().await;
    //     assert_eq!(records.len(), 0);
    // }



}