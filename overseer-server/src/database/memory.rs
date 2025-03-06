use std::{borrow::Borrow, cell::RefCell, collections::HashMap, iter::Map, marker::PhantomData, rc::Rc, sync::Arc};

use dashmap::DashMap;
use monoio::io::{as_fd::AsWriteFd, AsyncWriteRent, AsyncWriteRentExt};
use overseer::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, LocalReadAsync, Value}};

use overseer::network::OverseerSerde;
use crate::net::ClientId;

use super::watcher::{WatchClient, WatchServer, Watcher};



pub struct MemoryDatabase {
    /// The database list of records.
    records: RefCell<HashMap<Key, Record>>,
    /// The list of watchers.
    watchers: DashMap<Key, DashMap<ClientId, Watcher<WatchServer>>>
}

pub struct Record {
    value: Rc<Value>
}


impl Record {
    pub fn new(value: Value) -> Self {
        Self {
            value: Rc::new(value)
        }
    }
    pub async fn write<W>(&self, writer: &mut W) -> Result<(), NetworkError>
    where 
        W: tokio::io::AsyncWrite + Unpin
    {
        self.value().serialize(writer).await?;
        Ok(())
    }
    pub async fn read<R>(reader: &mut R) -> Result<Self, NetworkError>
    where 
        R: LocalReadAsync
    {
        Ok(Self {
            value: Rc::new(Value::deserialize(reader).await?)
        })
    }
    pub fn value(&self) -> &Rc<Value> {
        &self.value
    }
}

impl MemoryDatabase {
    pub fn new() -> Self {
        Self {
            records: RefCell::new(HashMap::new()),
            watchers: DashMap::new(),
        }
    }
    
    pub async fn insert<K, V>(&self, key: K, value: V)
    where 
        K: Borrow<Key>,
        V: Into<Value>
    {
        // let wow = *self.records.get(&key).unwrap();
        let key = key.borrow();
        let value = Rc::new(value.into());
        self.records.borrow_mut().insert(key.clone(), Record {
            value: Rc::clone(&value)
        });
        self.notify(key, Some(value)).await;
    }

    pub fn len(&self) -> usize {
        self.records.borrow().len()
    }
    pub async fn subscribe<K>(&self, key: K, client_id: ClientId, behaviour: WatcherBehaviour, activity: WatcherActivity) -> Watcher<WatchClient>
        where 
            K: Borrow<Key>
    {
        let key= key.borrow();
        let (client, server) = Watcher::new(behaviour);
        
        if let WatcherActivity::Kickback = activity {
            // Kick the value back immediately.
            server.wake(self.get(&key).await);
        }
        
        match self.watchers.get(&key) {
            Some(map) => {
                map.insert(client_id, server);
            },
            None => {
                // Not in the map.
                let map = DashMap::new();
                map.insert(client_id, server);
                self.watchers.insert(key.clone(), map.into());
            }
        }

        

        client
    }
    pub async fn release<K>(&self, key: K, id: ClientId) -> bool
    where 
        K: Borrow<Key>
    {
        let key = key.borrow();
        if self.watchers.contains_key(&key) {
            let value = self.watchers.get(&key).unwrap();
            if let Some((_, killed)) = value.remove(&id) {
                killed.kill();
                true
            } else {
                false
            }

        } else {
            false
        }
    }
    pub async fn notify<K>(&self, key: K, value: Option<Rc<Value>>) -> bool
    where 
        K: Borrow<Key>
    {
        match self.watchers.get(key.borrow()) {
            Some(map) => {
                Watcher::notify_coordinated(map.iter(), value);
                true
            },
            None => false
        }
    }
    pub async fn delete(&self, key: &Key) -> bool {
        if self.len() == 0 {
            return false;
        } else {
            if self.records.borrow_mut().remove(key).is_some() {
                self.notify(key, None).await;
                true
            } else {
                false
            }
        }
    }
    pub async fn get(&self, key: &Key) -> Option<Rc<Value>> {
        Some(Rc::clone(self.records.borrow().get(key)?.value()))
    }
}




#[cfg(test)]
mod tests {
    use std::sync::Arc;

    // use overseer::{access::{WatcherActivity, WatcherBehaviour}, models::{Key, Value}};
    // use tokio::sync::Notify;

    // use crate::{database::MemoryDatabase, net::ClientId};



    // #[tokio::test]
    // pub async fn test_db_insert_delete() {
    //     let db = MemoryDatabase::new();

    //     let key = Key::from_str("hello");
    //     db.insert(key.clone(), Value::Integer(12)).await;

    //     assert_eq!(db.len(), 1);

    //     assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 12);
    //     assert!(db.delete(&key).await);
    //     assert!(db.get(&key).await.is_none());

    //     assert_eq!(db.len(), 0);

    //     db.insert(key.clone(), Value::Integer(29)).await;
    //     assert_eq!(db.len(), 1);
    //     assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 29);
    //     db.insert(key.clone(), Value::Integer(30)).await;
    //     assert_eq!(db.len(), 1);
    //     assert_eq!(db.get(&key).await.unwrap().as_integer().unwrap(), 30);

    //     db.insert(Key::from_str("h2"), Value::Integer(13)).await;
    //     assert_eq!(db.len(), 2);
    //     assert_eq!(db.get(&Key::from_str("h2")).await.unwrap().as_integer().unwrap(), 13);

    //     assert!(db.delete(&Key::from_str("h2")).await);
    //     assert_eq!(db.len(), 1);
        
     

    // }

    // #[tokio::test]
    // pub async fn test_subscribe() {
    //     let db = Arc::new(MemoryDatabase::new());

    //     let is_up = Arc::new(Notify::new());

    //     let handle = tokio::spawn({
    //         let db = Arc::clone(&db);
    //         let is_up = Arc::clone(&is_up);
    //         async move {

    //             is_up.notify_one();

    //             let subbed = db.subscribe(Key::from_str("hello"), ClientId::from_id(0), WatcherBehaviour::Ordered, WatcherActivity::Lazy).await;
                

    //             let mut status = true;


    //             // The first change we want is when it is set.
    //             let value = subbed.wait().await;
    //             status = status & (*value.unwrap() == Value::Integer(0));

    //             // The first change we want is when it gets a new value.
    //             let value = subbed.wait().await;
    //             status = status & (*value.unwrap() == Value::Integer(1));

    //             // Then it gets deleted.
    //             status = status & (subbed.wait().await.is_none());



    //             status
    //         }
    //     });

    //     is_up.notified().await;

    //     // Insert the value.
    //     db.insert(Key::from_str("hello"), Value::Integer(0)).await;

    //     db.insert(Key::from_str("hello"), Value::Integer(1)).await;

    //     db.delete(&Key::from_str("hello")).await;

    //     assert!(handle.await.unwrap());
     

    // }


    // #[tokio::test]
    // /// This tests a complex setup with many watchers.
    // pub async fn many_watchers() {
    //     const KEY: &str = "config.kafka.brokers";

    //     let db = MemoryDatabase::new();

    //     // println!("Make DB");

    //     let mut handles = vec![];
    //     for i in 0..100 {
    //         handles.push(tokio::spawn({
    //             let watcher = db.subscribe(Key::from_str(KEY),  ClientId::from_id(i), WatcherBehaviour::Ordered, WatcherActivity::Lazy).await;
    //             async move {
    //                 assert_eq!(watcher.wait().await.unwrap().as_integer().unwrap(), 12);
    //                 // println!("Seen the first");
    //                 assert_eq!(watcher.wait().await.unwrap().as_integer().unwrap(), 6);
    //                 // println!("Seen the secon.");
    //             }
    //         }));
    //     }


    //     // We start out with twelve brokers.
    //     db.insert(Key::from_str(KEY), 12).await;

    //     // println!("Inserted");

    //     // Now let us scale down to 6.
    //     db.insert(Key::from_str(KEY), 6).await;


    //     for handle in handles {
    //         handle.await.unwrap();
    //     }
        


    // }

    // #[tokio::test]
    // pub async fn test_watcher_kickback_versus_lazy() {
    //     const KEY: &str = "database.pool.size";

    //     let db = MemoryDatabase::new();
    //     db.insert(Key::from_str(KEY), 0).await;

    //     let lazy = db.subscribe(Key::from_str(KEY), ClientId::from_id(0), WatcherBehaviour::Eager, WatcherActivity::Lazy).await;
    //     let kickback = db.subscribe(Key::from_str(KEY), ClientId::from_id(0), WatcherBehaviour::Eager, WatcherActivity::Kickback).await;

    //     // The kickback should have the value.
    //     assert!(lazy.force_recv().await.is_none());
    //     assert_eq!(kickback.force_recv().await.unwrap().as_integer().unwrap(), 0);


    // }

    // #[tokio::test]
    // /// This tests a complex setup with many watchers.
    // /// This test specifically deals with eager watchers, which means that they
    // /// can see either value (depending on when they receive it.)
    // pub async fn many_eager_watchers() {
    //     const KEY: &str = "config.kafka.brokers";

    //     let db = MemoryDatabase::new();

    //     let mut handles = vec![];
    //     for i in 0..100 {
    //         handles.push(tokio::spawn({
    //             let watcher = db.subscribe(Key::from_str(KEY),  ClientId::from_id(i), WatcherBehaviour::Eager, WatcherActivity::Lazy).await;
    //             async move {

    //                 let int = watcher.wait().await.unwrap().as_integer().unwrap();
    //                 assert!(int == 12 || int == 6);
    //             }
    //         }));
    //     }


    //     // We start out with twelve brokers.
    //     db.insert(Key::from_str(KEY), 12).await;

    //     // Now let us scale down to 6.
    //     db.insert(Key::from_str(KEY), 6).await;


    //     for handle in handles {
    //         handle.await.unwrap();
    //     }
        


    // }
}