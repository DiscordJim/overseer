use std::{path::Path, str::FromStr, time::Duration};

use overseer::{error::NetworkError, models::{Key, Value}};
use sqlx::{sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow}, Pool, Sqlite, SqlitePool};

use sqlx::Row;


/// The storage driver for the database. Without this we cannot store things.
pub struct DatabaseStorage
{
    pool: Pool<Sqlite>
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
 
        let conn_str = &("sqlite://".to_string() + path.to_str().unwrap());
    
        let pool = SqlitePool::connect_with(SqliteConnectOptions::from_str(conn_str)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(2))
            .journal_mode(SqliteJournalMode::Wal)
            
        
        ).await?;
        
        setup_table(&pool).await?;

        Ok(Self {
            pool
        })
    }
    pub async fn write(&self, key: &Key, value: &Value) -> Result<(), NetworkError> {

        sqlx::query("INSERT INTO kv_table(key, type, data) VALUES ($1, $2, $3)")
            .bind(key.as_str())
            .bind(value.discriminator())
            .bind(value.as_bytes())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn update(&self, key: &Key, value: Value) -> Result<(), NetworkError> {
        sqlx::query("UPDATE kv_table SET type = $1, data = $2  WHERE key = $3")
            .bind(value.discriminator())
            .bind(value.as_bytes())
            .bind(key.as_str())
            .execute(&self.pool).await?;

        Ok(())

    }
    pub async fn delete(&self, key: &Key) -> Result<(), NetworkError> {
        sqlx::query("DELETE FROM kv_table WHERE key = $1")
            .bind(key.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn read(&self) -> Result<Vec<StoredRecord>, NetworkError> {
        let rows = sqlx::query("SELECT * FROM kv_table;")
            .fetch_all(&self.pool).await?
            .into_iter().map(read_sqliterow)
            .collect::<Result<Vec<_>, NetworkError>>()?;
        Ok(rows)
    }
}


fn read_sqliterow(row: SqliteRow) -> Result<StoredRecord, NetworkError> {
    let r = row.get::<String, _>(1);
    let v_type = row.get::<i64, _>(2);
    let bytes = row.get::<Vec<u8>, _>(3);

    Ok(StoredRecord {
        key: r.into(),
        value: Value::decode(v_type as u8, &bytes)?
    })
}




async fn setup_table(pool: &Pool<Sqlite>) -> Result<(), NetworkError>
{

    sqlx::query("CREATE TABLE IF NOT EXISTS kv_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT,
    type INTEGER,
    data BLOB
    );").execute(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use overseer::models::{Key, Value};

    use crate::database::DatabaseStorage;


    #[tokio::test]
    pub async fn test_db_rw_record() {
        let tf = tempfile::tempdir().unwrap();
        let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
        da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

        let records = da.read().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().unwrap().value, Value::Integer(21));
    }

    #[tokio::test]
    pub async fn test_db_update_record() {
        let tf = tempfile::tempdir().unwrap();
        let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
        da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

        da.update(&Key::from_str("hello"), Value::Integer(23)).await.unwrap();

        let records = da.read().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().unwrap().value, Value::Integer(23));
    }

    #[tokio::test]
    pub async fn test_db_rw_record_delete() {
        let tf = tempfile::tempdir().unwrap();
        let da = DatabaseStorage::new(tf.path(), "test.sqlite").await.unwrap();
        da.write(&Key::from_str("hello"), &Value::Integer(21)).await.unwrap();

        da.delete(&Key::from_str("hello")).await.unwrap();

        let records = da.read().await.unwrap();
        assert_eq!(records.len(), 0);
    }



}