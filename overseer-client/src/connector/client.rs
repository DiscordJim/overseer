use std::{borrow::Borrow, net::{SocketAddr, ToSocketAddrs}, sync::Mutex};

use overseer::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}, network::Packet};
use tokio::net::TcpStream;



pub struct Client {
    address: SocketAddr,
    connection: Mutex<Option<TcpStream>>
}

struct ClientInternal {

}

impl Client {

    pub async fn new<A>(address: A) -> Result<Self, NetworkError>
    where 
        A: ToSocketAddrs
    {
        let address = address.to_socket_addrs().map_err(|_| NetworkError::SocketError)?.nth(0).unwrap();
        Ok(Self {
            address,
            connection: Mutex::new(None)
        })
    }
    async fn connect(&self) -> Result<(), NetworkError> {
        if self.connection.lock().unwrap().is_none() {
            let stream = TcpStream::connect(self.address).await?;
            *self.connection.lock().unwrap() = Some(stream);
        }
        Ok(())
    }
    async fn send(&self, packet: Packet) -> Result<Packet, NetworkError> {
        let mut handle = self.connection.lock().unwrap();
        let stream = handle.as_mut().unwrap();
        packet.write(stream).await?;
        Ok(Packet::read(stream).await?)
    }
    pub async fn get<K>(&self, key: K) -> Result<Option<Value>, NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

   
        if let Packet::Return { value, .. } = self.send(Packet::get(key)).await? {
            return Ok(value);
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
    }
    pub async fn delete<K>(&self, key: K) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

   
        if let Packet::Get { .. } = self.send(Packet::delete(key)).await? {
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

        if let Packet::Return { value, .. } = self.send(Packet::insert(key, value)).await? {
            return Ok(value);
        } else {
            return Err(NetworkError::WrongResponseFromServer);
        }
    }
    pub async fn subscribe<K>(&self, key: K, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Result<(), NetworkError>
    where 
        K: Borrow<Key>
    {
        self.connect().await?;

        self.send(Packet::watch(key, activity, behaviour)).await?;

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::Client;


   
}