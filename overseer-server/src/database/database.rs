use std::{borrow::Borrow, path::Path, rc::Rc, sync::Arc};

use overseer::{
    access::{WatcherActivity, WatcherBehaviour},
    error::NetworkError,
    models::{Key, Value},
};

use crate::net::ClientId;

use super::{DatabaseStorage, MemoryDatabase, WatchClient, Watcher};


/// The [Database] structure which controls the API to the
/// underlying key-value store.
pub struct Database {
    /// The memory backend.
    memory: MemoryDatabase,
    /// The storage backend.
    storage: DatabaseStorage,
}

impl Database {
    /// Creates a new database at a path and with a specific name.
    /// 
    /// Internally, there is a memory component and a storage component
    /// for persistence. This just coordinates the two backends.
    pub async fn new<P, S>(path: P, name: S) -> Result<Self, NetworkError>
    where
        P: AsRef<Path>,
        S: AsRef<str>,
    {
        let storage = DatabaseStorage::new(path, name).await?;
        let memory = MemoryDatabase::new();

        for (key, value) in storage.records().await {
            memory.insert(key, value).await;
        }

        Ok(Self { memory, storage })
    }
    /// Gets a value for a key.
    pub async fn get<K>(&self, key: K) -> Option<Rc<Value>>
    where
        K: Borrow<Key>,
    {
        self.memory.get(key.borrow()).await
    }
    /// Inserts a value under a key.
    pub async fn insert<K>(&self, key: K, value: Value) -> Result<(), NetworkError>
    where
        K: Borrow<Key>,
    {
        self.storage.write(key.borrow(), &value).await?;
        self.memory.insert(key.borrow(), value).await;
        Ok(())
    }
    /// Deletes a value under a key.
    pub async fn delete<K>(&self, key: K) -> Result<(), NetworkError>
    where
        K: Borrow<Key>,
    {
        self.storage.delete(key.borrow()).await?;
        self.memory.delete(key.borrow()).await;
        Ok(())
    }
    /// Releases a subscription.
    pub async fn release<K>(&self, key: K, id: ClientId) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.memory.release(key, id).await;
        Ok(())
    }
    /// Subscribes to a key.
    pub async fn subscribe<K>(
        &self,
        key: K,
        client: ClientId,
        behaviour: WatcherBehaviour,
        activity: WatcherActivity,
    ) -> Result<Watcher<WatchClient>, NetworkError>
    where
        K: Borrow<Key>,
    {
        Ok(self
            .memory
            .subscribe(key, client, behaviour, activity)
            .await)
    }
}

#[cfg(test)]
mod tests {
    use overseer::models::{Key, Value};

    use crate::database::Database;

    // #[tokio::test]
    // pub async fn test_database_persistence() {
    //     let tf = tempfile::tempdir().unwrap();
    //     let da = Database::new(tf.path(), "test.sqlite").await.unwrap();

    //     // Insert a record.
    //     da.insert(Key::from_str("hello"), Value::Integer(21))
    //         .await
    //         .unwrap();

    //     // Reopen the database.
    //     let da = Database::new(tf.path(), "test.sqlite").await.unwrap();
    //     assert_eq!(
    //         *da.get(Key::from_str("hello")).await.unwrap(),
    //         Value::Integer(21)
    //     );
    // }

    // #[tokio::test]
    // pub async fn test_hot_cold() {
    //     let tf = tempfile::tempdir().unwrap();
    //     let da = Database::new(tf.path(), "test.sqlite").await.unwrap();

    //     // Insert a record.
    //     da.insert(Key::from_str("hello"), Value::Integer(21))
    //         .await
    //         .unwrap();

    //     // Reopen the database.
    //     let da = Database::new(tf.path(), "test.sqlite").await.unwrap();
    //     assert_eq!(
    //         *da.get(Key::from_str("hello")).await.unwrap(),
    //         Value::Integer(21)
    //     );
    // }
}
