use std::{borrow::Borrow, sync::{Arc, RwLock}};

use overseer::models::{Key, Value};
use whirlwind::ShardMap;

use super::watcher::{WatchClient, WatchServer, Watcher, WatcherBehaviour};



pub struct Database {
    /// The database list of records.
    records: ShardMap<Key, Record>,
    /// The list of watchers.
    watchers: ShardMap<Key, RwLock<Vec<Watcher<WatchServer>>>>
}

pub struct Record {
    value: Arc<Value>
}

pub struct DbRecord//(Arc<(Key, RwLock<Option<Record>>)>); 
{
    key: Key,
    record: Record
}

impl DbRecord {
    pub fn new(key: Key, value: Arc<Value>) -> Self {

        Self {
            key: key.clone(),
            record: Record { value }
        }
    }
    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn value_unchecked(&self) -> Arc<Value> {
        Arc::clone(&self.record.value)
    }
}

impl Database {
    pub fn new() -> Self {
        Self {
            records: ShardMap::new(),
            watchers: ShardMap::new(),
        }
    }
    
    pub async fn insert<K, V>(&self, key: K, value: V)
    where 
        K: Borrow<Key>,
        V: Into<Value>
    {
        let value = Arc::new(value.into());
        self.records.insert(key.borrow().clone(), Record {
            value: Arc::clone(&value)
        }).await;
        self.notify(key.borrow(), Some(value)).await;
    }

    pub async fn len(&self) -> usize {
        self.records.len().await
    }
    pub async fn subscribe(&self, key: Key, behaviour: WatcherBehaviour) -> Watcher<WatchClient> {
        let (client, server) = Watcher::new(behaviour);
        if !self.watchers.contains_key(&key).await {
            self.watchers.insert(key.clone(), RwLock::new(vec![server])).await;
        } else {
            let obj = self.watchers.get(&key).await;
            obj.unwrap().write().unwrap().push(server);
        }
        client
    }
    pub async fn notify(&self, key: &Key, value: Option<Arc<Value>>) -> bool {
        if !self.watchers.contains_key(&key).await {
            false
        } else {
            for watcher in &*self.watchers.get(&key).await.unwrap().read().unwrap() {
                watcher.wake(value.clone());
            }
            true
        }
    }
    pub async fn delete(&self, key: &Key) -> bool {
        if self.len().await == 0 {
            return false;
        } else {
            if self.records.remove(key).await.is_some() {
                self.notify(key, None).await;
                true
            } else {
                false
            }
        }
    }
    pub async fn get(&self, key: &Key) -> Option<Arc<Value>> {
        Some(self.records.get(key).await?.value().value.clone())
    }
}



#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use overseer::models::{Key, Value};
    use tokio::sync::Notify;

    use crate::database::{watcher::WatcherBehaviour, Database};


    #[tokio::test]
    pub async fn test_db_insert_delete() {
        let db = Database::new();

        let key = Key::from_str("hello");
        db.insert(key.clone(), Value::Integer(12)).await;

        assert_eq!(db.len().await, 1);

        assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 12);
        assert!(db.delete(&key).await);
        assert!(db.get(&key).await.is_none());

        assert_eq!(db.len().await, 0);

        db.insert(key.clone(), Value::Integer(29)).await;
        assert_eq!(db.len().await, 1);
        assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 29);
        db.insert(key.clone(), Value::Integer(30)).await;
        assert_eq!(db.len().await, 1);
        assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 30);

        db.insert(Key::from_str("h2"), Value::Integer(13)).await;
        assert_eq!(db.len().await, 2);
        assert_eq!(db.get(&Key::from_str("h2")).await.unwrap().as_integer().unwrap(), 13);

        assert!(db.delete(&Key::from_str("h2")).await);
        assert_eq!(db.len().await, 1);
        
     

    }

    #[tokio::test]
    pub async fn test_subscribe() {
        let db = Arc::new(Database::new());

        let is_up = Arc::new(Notify::new());

        let handle = tokio::spawn({
            let db = Arc::clone(&db);
            let is_up = Arc::clone(&is_up);
            async move {

                is_up.notify_one();

                let subbed = db.subscribe(Key::from_str("hello"), WatcherBehaviour::Ordered).await;
                

                let mut status = true;


                // The first change we want is when it is set.
                let value = subbed.wait().await;
                status = status & (*value.unwrap() == Value::Integer(0));

                // The first change we want is when it gets a new value.
                let value = subbed.wait().await;
                status = status & (*value.unwrap() == Value::Integer(1));

                // Then it gets deleted.
                status = status & (subbed.wait().await.is_none());



                status
            }
        });

        is_up.notified().await;

        // Insert the value.
        db.insert(Key::from_str("hello"), Value::Integer(0)).await;

        db.insert(Key::from_str("hello"), Value::Integer(1)).await;

        db.delete(&Key::from_str("hello")).await;

        assert!(handle.await.unwrap());
     

    }


    #[tokio::test]
    /// This tests a complex setup with many watchers.
    pub async fn many_watchers() {
        const KEY: &str = "config.kafka.brokers";

        let db = Database::new();

        let mut handles = vec![];
        for _ in 0..100 {
            handles.push(tokio::spawn({
                let watcher = db.subscribe(Key::from_str(KEY), WatcherBehaviour::Ordered).await;
                async move {
                    assert_eq!(watcher.wait().await.unwrap().as_integer().unwrap(), 12);
                    assert_eq!(watcher.wait().await.unwrap().as_integer().unwrap(), 6);
                }
            }));
        }


        // We start out with twelve brokers.
        db.insert(Key::from_str(KEY), 12).await;

        // Now let us scale down to 6.
        db.insert(Key::from_str(KEY), 6).await;


        for handle in handles {
            handle.await.unwrap();
        }
        


    }
}