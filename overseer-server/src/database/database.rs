use std::{collections::VecDeque, path::PathBuf, sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex, RwLock}};

use overseer::models::{Key, Value};
use tokio::sync::Notify;
use whirlwind::ShardMap;



pub struct Database {
    tombstoned: AtomicUsize,

    records: RwLock<Vec<DbRecord>>,

    watchers: ShardMap<Key, RwLock<Vec<Watcher>>>
}

pub enum WatcherBehaviour {
    /// This type of watcher will immediately get an update sent to it with the current
    /// state.
    Kickback,
    /// This type of watcher will only be updated once there is new behaviour past registration.
    Passive
}

#[derive(Clone)]
pub struct Watcher {
    target: Key,
    triggered: Arc<Mutex<VecDeque<Option<Arc<Value>>>>>,
    // triggered: Arc<Mutex<Option<Value>>>,
    wakeup: Arc<Notify>
}

impl Watcher {
    pub fn new(key: Key) -> Self {
        Self {
            target: key,
            triggered: Arc::default(),
            wakeup: Arc::default()
        }
    }
    pub async fn wait(&self) -> Option<Arc<Value>> {
        if !self.triggered.lock().unwrap().is_empty() {
            self.triggered.lock().unwrap().pop_front()?
        } else {
            self.wakeup.notified().await;
            let popped = self.triggered.lock().unwrap().pop_front()?;
            println!("Wokeup with {:?}", popped);
            popped
        }
    }
    pub fn wake(&self, value: Option<Arc<Value>>) {
        println!("Woken with {:?}", value);
        self.triggered.lock().unwrap().push_back(value);
        self.wakeup.notify_one();
    }
}

pub struct Record {
    key: Key,
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
                key,
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
                *lock = Some(Record { key: self.key().clone(), value }.into())
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
            records: Vec::new().into(),
            watchers: ShardMap::new(),
            tombstoned: AtomicUsize::new(0)
        }
    }
    
    pub async fn insert(&self, key: Key, value: Value) {

        let notif = key.clone();
        let value = Arc::new(value);
        
        let read_lock = self.records.read().unwrap();
        if let Some((key, value)) = insert_tombstone_or_update(&read_lock, &self.tombstoned, key, &value) {
            // This must be a new record that we had no empty slots to insert in.
            drop(read_lock);
            self.records.write().unwrap().push(DbRecord::new(key.clone(), Arc::clone(&value)));
        }
        // println!("Notifying with {:?}", val);
        self.notify(&notif, Some(value)).await;
    }
    pub fn len(&self) -> usize {
        self.records.read().unwrap().len() - self.tombstoned.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn subscribe(&self, key: Key) -> Watcher {
        let watcher = Watcher::new(key.clone());
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
        if self.len() == 0 {
            return false;
        } else {
            for record in &*self.records.read().unwrap() {
                if record.key() == key {
                    self.tombstoned.fetch_add(1, Ordering::Release);
                    record.tombstone();
                    return true;
                }
            }
        }

        self.notify(key, None).await;
        
        // Could not find the entry.
        false
    }
    pub fn get(&self, key: &Key) -> Option<Arc<Value>> {
        let lock = self.records.read().unwrap();
        let found = lock.iter().find(|f| f.key() == key)?;
        if found.is_tombstone() {
            None
        } else {
            Some(found.value_unchecked())
        }
    }
}


fn insert_tombstone_or_update<'a>(master: &[DbRecord], tbs: &AtomicUsize, key: Key, value: &'a Arc<Value>) -> Option<(Key, &'a Arc<Value>)> {

    let mut first_tombstone = None;

    for (pos, record) in master.iter().enumerate() {
        if record.is_tombstone() && first_tombstone.is_none() {
            first_tombstone = Some(pos);

        }
        if *record.key() == key && !record.is_tombstone() {
            // Update the existing record.
            record.update(Arc::clone(value));
            return None;
        }
        
    }

    // Still have not found a record. Insert at tombstone
    // if possible.
    if let Some(pos) = first_tombstone {

        tbs.fetch_sub(1, Ordering::Release);
        master[pos].update(Arc::clone(value));
        return None;
    }


    Some((key, value))
}

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

        assert_eq!(db.len(), 1);

        assert_eq!(db.get(&key).unwrap().as_integer().unwrap(), 12);
        assert!(db.delete(&key).await);
        assert!(db.get(&key).is_none());

        assert_eq!(db.len(), 0);

        db.insert(key.clone(), Value::Integer(29)).await;
        assert_eq!(db.len(), 1);
        assert_eq!(db.get(&key).unwrap().as_integer().unwrap(), 29);
        db.insert(key.clone(), Value::Integer(30)).await;
        assert_eq!(db.len(), 1);
        assert_eq!(db.get(&key).unwrap().as_integer().unwrap(), 30);

        db.insert(Key::from_str("h2"), Value::Integer(13)).await;
        assert_eq!(db.len(), 2);
        assert_eq!(db.get(&Key::from_str("h2")).unwrap().as_integer().unwrap(), 13);

        assert!(db.delete(&Key::from_str("h2")).await);
        assert_eq!(db.len(), 1);
        
     

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