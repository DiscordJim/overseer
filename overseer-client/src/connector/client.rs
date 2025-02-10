use std::{borrow::Borrow, net::{SocketAddr, ToSocketAddrs}, sync::{atomic::{AtomicBool, AtomicU32, Ordering}, Arc}};

use dashmap::DashMap;
use overseer::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}, network::{Packet, PacketId, PacketPayload}};
use tokio::{net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}, sync::{oneshot::Sender, Mutex, Notify}};

use tokio::io::AsyncWriteExt;
#[derive(Clone)]
pub struct LiveValue {
    value: Arc<LiveValueInternal>,

}


impl LiveValue {
    pub async fn get(&self) -> Option<Value> {
        self.value.value.lock().await.clone()
    }
    pub async fn wait_on_update(&self) -> Option<Value> {
        self.value.notify.notified().await;
        self.get().await
    }
}

struct LiveValueInternal {
    value: Mutex<Option<Value>>,
    notify: Notify
}

pub struct Client {
    address: SocketAddr,
    inner: Arc<Inner>
}

struct Inner {
    write: Mutex<Option<(OwnedWriteHalf, Arc<Notify>)>>,
    counter: AtomicU32,
    signal: Notify,
    channels: DashMap<u32, Sender<Packet>>,
    watched: DashMap<Key, LiveValue>
    // channel: 
}





async fn run_client_backend(mut read: OwnedReadHalf, kill: Arc<Notify>, inner: Arc<Inner>) -> Result<(), NetworkError>
{

    inner.signal.notify_waiters();

    loop {

        let packet = tokio::select! {
            e = Packet::read(&mut read) => {
                e
            },
            _ = kill.notified() => {
                break;
            }
        }?;
        let packet_id = packet.id();

 
        if packet_id.id() == 0 {
            if let PacketPayload::Notify { key, value, .. } = packet.payload() {
                let live_value = &*inner.watched.get(key).unwrap().value;
                *live_value.value.lock().await = value.clone();
                live_value.notify.notify_waiters();
            }
        } else {
            let (_, channel) = inner.channels.remove(&packet_id.id()).unwrap();
            channel.send(packet).unwrap();
        }

    }
    println!("Hello broken");

    Ok(())
}

impl Client {

    pub async fn new<A>(address: A) -> Result<Self, NetworkError>
    where 
        A: ToSocketAddrs
    {
        println!("HELLO");
        let address = address.to_socket_addrs().map_err(|_| NetworkError::SocketError)?.nth(0).unwrap();
        Ok(Self {
            address,
            inner: Arc::new(Inner {
                counter: AtomicU32::new(1),
                write: Mutex::new(None),
                signal: Notify::new(),
                channels: DashMap::new(),
                watched: DashMap::new()
            })
        })
    }
    pub async fn reset_connection(&self) -> Result<(), NetworkError> {
        if let Some((a, kill)) = &mut *self.inner.write.lock().await {
            a.shutdown().await?;
            kill.notify_waiters();
        }
        *self.inner.write.lock().await = None;
        Ok(())

    }
    async fn connect(&self) -> Result<(), NetworkError> {
        println!("CONNECT");
        if self.inner.write.lock().await.is_none() {
            let (read, write) = TcpStream::connect(self.address).await?.into_split();

            let notif = Arc::new(Notify::new());

            tokio::spawn(run_client_backend(read, notif.clone(), Arc::clone(&self.inner)));
            
            self.inner.signal.notified().await;
            *self.inner.write.lock().await = Some((write, notif));
            
        }
        Ok(())
    }
    async fn send(&self, packet: Packet) -> Result<Packet, NetworkError> {
        let mut handle = self.inner.write.lock().await;
        let (stream, _) = handle.as_mut().unwrap();


        let (sdr, rcv) = tokio::sync::oneshot::channel::<Packet>();
        
        self.inner.channels.insert(packet.id().id(), sdr);

        packet.write(stream).await?;
        Ok(rcv.await.unwrap())
        // Ok(Packet::read(stream).await?)
    }
    fn count(&self) -> u32 {
        self.inner.counter.fetch_add(1, Ordering::AcqRel)
    }
    pub async fn get<K>(&self, key: K) -> Result<Option<Value>, NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

        let packet = Packet::new(PacketId::new(self.count(), 0), PacketPayload::get(key));
        if let PacketPayload::Return { value, .. } = self.send(packet).await?.payload() {
            return Ok(value.clone());
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
    }
    pub async fn delete<K>(&self, key: K) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

   
        let packet = Packet::new(PacketId::new(self.count(), 0), PacketPayload::delete(key));
        if let PacketPayload::Get { .. } = self.send(packet).await?.payload() {
            return Ok(());
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
    }
    pub async fn insert<K>(&self, key: K, value: Value) -> Result<Option<Value>, NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

        // if let Packet::Return { value, .. } = self.send(Packet::insert(key, value)).await? {
        //     return Ok(value);
        // } else {
        //     return Err(NetworkError::WrongResponseFromServer);
        // }
        println!("Hello");
        let packet = Packet::new(PacketId::new(self.count(), 0), PacketPayload::insert(key, value));
        if let PacketPayload::Return { value, .. } = self.send(packet).await?.payload() {
            return Ok(value.clone());
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
        // Ok(None)
    }
    pub async fn subscribe<K>(&self, key: K, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Result<LiveValue, NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

        let inner = LiveValue {
            value: Arc::new(LiveValueInternal {
                value: Mutex::default(),
                notify: Notify::new()
            })
        };

        self.inner.watched.insert(key.borrow().clone(), inner.clone());
        let packet = Packet::new(PacketId::new(self.count(), 0), PacketPayload::watch(key, activity, behaviour));
        
        if let PacketPayload::Get { .. } = self.send(packet).await?.payload() {
            return Ok(inner);
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
        // self.send(Packet::watch(key, activity, behaviour)).await?;

        // Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::Client;


   
}