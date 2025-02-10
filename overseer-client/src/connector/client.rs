use std::{borrow::Borrow, net::{SocketAddr, ToSocketAddrs}, sync::{Arc, Mutex}};

use overseer::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}, network::Packet};
use tokio::net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream};



pub struct Client {
    address: SocketAddr,
    inner: Arc<Inner>
}

struct Inner {
    write: Mutex<Option<OwnedWriteHalf>>,
    // channel: 
}


async fn run_client_backend(read: OwnedReadHalf, inner: Arc<Inner>)
{

}

impl Client {

    pub async fn new<A>(address: A) -> Result<Self, NetworkError>
    where 
        A: ToSocketAddrs
    {
        let address = address.to_socket_addrs().map_err(|_| NetworkError::SocketError)?.nth(0).unwrap();
        Ok(Self {
            address,
            inner: Arc::new(Inner {
                write: Mutex::new(None)
            })
        })
    }
    async fn connect(&self) -> Result<(), NetworkError> {
        if self.inner.write.lock().unwrap().is_none() {
            let (read, write) = TcpStream::connect(self.address).await?.into_split();
            *self.inner.write.lock().unwrap() = Some(write);
            tokio::spawn(run_client_backend(read, Arc::clone(&self.inner)));
        }
        Ok(())
    }
    async fn send(&self, packet: Packet) -> Result<(), NetworkError> {
        let mut handle = self.inner.write.lock().unwrap();
        let stream = handle.as_mut().unwrap();
        packet.write(stream).await?;
        Ok(())
        // Ok(Packet::read(stream).await?)
    }
    pub async fn get<K>(&self, key: K) -> Result<Option<Value>, NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

   
        // if let Packet::Return { value, .. } = self.send(Packet::get(key)).await? {
        //     return Ok(value);
        // } else {
        //     return Err(NetworkError::WrongResponseFromServer);
        // }
        Ok(None)
    }
    pub async fn delete<K>(&self, key: K) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

   
        // if let Packet::Get { .. } = self.send(Packet::delete(key)).await? {
        //     return Ok(());
        // } else {
        //     return Err(NetworkError::WrongResponseFromServer);
        // }
        Ok(())
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
        Ok(None)
    }
    pub async fn subscribe<K>(&self, key: K, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

        // self.send(Packet::watch(key, activity, behaviour)).await?;

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::Client;


   
}