use std::{borrow::Borrow, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock}};

use overseer::models::{Key, Value};
use whirlwind::ShardMap;

use super::watcher::Watcher;



pub struct Database {
    /// How many entries in the database are currently tombstoned?
    tombstoned: AtomicUsize,
    /// The database list of records.
    records: ShardMap<Key, Record>,
    /// The list of watchers.
    watchers: ShardMap<Key, RwLock<Vec<Watcher>>>
}

pub struct Record {
    value: Arc<Value>
}

pub struct DbRecord//(Arc<(Key, RwLock<Option<Record>>)>); 
{
    key: Key,
    record: RwLock<Option<Record>>
}

impl DbRecord {
    pub fn new(key: Key, value: Arc<Value>) -> Self {

        Self {
            key: key.clone(),
            record: RwLock::new(Some(Record {
                value
            }))
        }
    }
    pub fn is_tombstone(&self) -> bool {
        self.record.read().unwrap().is_none()
    }
    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn update(&self, value: Arc<Value>) {
        let mut lock = self.record.write().unwrap();
        match &mut *lock {
            Some(re) => {
                re.value = value;
            },
            None => {
                *lock = Some(Record { value }.into())
            }
        }
        
    }
    pub fn value_unchecked(&self) -> Arc<Value> {
        Arc::clone(&self.record.read().unwrap().as_ref().unwrap().value)
        // self.record.read().unwrap().as_ref().unwrap().value.clone()
    }
    pub fn tombstone(&self) {
        *self.record.write().unwrap() = None;
    }
}

impl Database {
    pub fn new() -> Self {
        Self {
            records: ShardMap::new(),
            watchers: ShardMap::new(),
            tombstoned: AtomicUsize::new(0)
        }
    }
    
    pub async fn insert<K: Borrow<Key>>(&self, key: K, value: Value) {

        // let notif = key.clone();
        let value = Arc::new(value);

        self.records.insert(key.borrow().clone(), Record {
            value: Arc::clone(&value)
        }).await;

        
        
        // let read_lock = self.records.read().unwrap();
        // if let Some((key, value)) = insert_tombstone_or_update(&read_lock, &self.tombstoned, key, &value) {
        //     // This must be a new record that we had no empty slots to insert in.
        //     drop(read_lock);
        //     self.records.write().unwrap().push(DbRecord::new(key.clone(), Arc::clone(&value)));
        // }
        // println!("Notifying with {:?}", val);
        self.notify(key.borrow(), Some(value)).await;
    }
    pub async fn len(&self) -> usize {
        self.records.len().await
    }
    pub async fn subscribe(&self, key: Key) -> Watcher {
        let watcher = Watcher::new();
        if !self.watchers.contains_key(&key).await {
            self.watchers.insert(key.clone(), RwLock::new(vec![watcher.clone()])).await;
        } else {
            let obj = self.watchers.get(&key).await;
            obj.unwrap().write().unwrap().push(watcher.clone());
        }
        watcher
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


// fn insert_tombstone_or_update<'a>(master: &[DbRecord], tbs: &AtomicUsize, key: Key, value: &'a Arc<Value>) -> Option<(Key, &'a Arc<Value>)> {

//     let mut first_tombstone = None;

//     for (pos, record) in master.iter().enumerate() {
//         if record.is_tombstone() && first_tombstone.is_none() {
//             first_tombstone = Some(pos);

//         }
//         if *record.key() == key && !record.is_tombstone() {
//             // Update the existing record.
//             record.update(Arc::clone(value));
//             return None;
//         }
        
//     }

//     // Still have not found a record. Insert at tombstone
//     // if possible.
//     if let Some(pos) = first_tombstone {

//         tbs.fetch_sub(1, Ordering::Release);
//         master[pos].update(Arc::clone(value));
//         return None;
//     }


//     Some((key, value))
// }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use overseer::models::{Key, Value};
    use tokio::sync::Notify;

    use crate::database::Database;


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

                let subbed = db.subscribe(Key::from_str("hello")).await;
                

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
}