use std::{net::SocketAddr, sync::Arc};

use overseer::{error::NetworkError, models::Key, network::decoder::Packet};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream, ToSocketAddrs,
    }, sync::mpsc::{Receiver, Sender},
};
use whirlwind::ShardMap;

use crate::database::{Database, WatchClient, Watcher};

pub struct Driver {
    internal: Arc<DriverInternal>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn from_id(i: u64) -> Self {
        Self(i)
    }
}

struct DriverInternal {
    database: Database,
    stream: TcpListener,
    write_queue: Arc<ShardMap<ClientId, Sender<Packet>>>
}

impl DriverInternal {
    pub async fn send(&self, id: ClientId, packet: Packet) {
        let queue = self.write_queue.get(&id).await.unwrap().value().clone();
        queue.send(packet).await.unwrap();
    }
}

impl Driver {
    pub async fn start<A: ToSocketAddrs>(addr: A) -> Result<Self, NetworkError> {
        let internal = Arc::new(DriverInternal {
            database: Database::new(),
            stream: TcpListener::bind(addr).await?,
            write_queue: ShardMap::new().into()
        });

        tokio::spawn({
            let internal = Arc::clone(&internal);
            async move {
                let mut counter = 0;
                loop {
                    let (sock, addr) = internal.stream.accept().await?;
                    handle_client(sock, addr, ClientId(counter), Arc::clone(&internal)).await;
                    counter += 1;
                }
                Ok::<(), NetworkError>(())
            }
        });

        Ok(Self {
            internal: Arc::clone(&internal),
        })
    }
    pub fn port(&self) -> u16 {
        self.internal.stream.local_addr().unwrap().port()
    }
}

async fn handle_client(socket: TcpStream, _address: SocketAddr, id: ClientId, internal: Arc<DriverInternal>) {
    println!("Spawning new client...");
    let (read, write) = socket.into_split();
    let (sender, receiver) = tokio::sync::mpsc::channel(250);
    internal.write_queue.insert(id, sender).await;
    let ctx = Arc::new(ClientContext {
        id,
        watches: ShardMap::new()
    });
    tokio::spawn(handle_client_write(write, receiver));
    tokio::spawn(handle_client_read(read, internal, ctx));
    
}

struct ClientContext {
    id: ClientId,
    watches: ShardMap<Key, Arc<Watcher<WatchClient>>>
}

async fn handle_client_write(
    mut socket: OwnedWriteHalf,
    mut receiver: Receiver<Packet>,
) -> Result<(), NetworkError> {
    loop {
        let packet = receiver.recv().await.unwrap();
        packet.write(&mut socket).await?;
    }
}

async fn handle_client_read(
    mut socket: OwnedReadHalf,
    internal: Arc<DriverInternal>,
    ctx: Arc<ClientContext>,
) -> Result<(), NetworkError> {
    loop {
        let packet = Packet::read(&mut socket).await?;
        match packet {
            Packet::Insert { key, value } => {
                internal.database.insert(key.clone(), value).await;
                internal.send(ctx.id, Packet::Get { key }).await;
            },
            Packet::Get { key } => {
                let value = { internal.database.get(&key).await }.map(|f| (*f).to_owned());
                internal.send(ctx.id, Packet::GetReturn { key, value }).await;
            },
            Packet::Delete { key } => {
                internal.database.delete(&key).await;
                internal.send(ctx.id, Packet::get(key)).await;
            },
            Packet::Watch { key, activity, behaviour } => {
                let wow = Arc::new(internal.database.subscribe(key.clone(), ctx.id, behaviour, activity).await);
                ctx.watches.insert(key.clone(), Arc::clone(&wow)).await;
                tokio::spawn({
                    let internal = Arc::clone(&internal);
                    let ctx = Arc::clone(&ctx);
                    async move {
                        spawn_subscriber(key, wow, internal, ctx).await;
                    }
                });
            },
            Packet::Release { key } => {
                if let Some(..) = ctx.watches.remove(&key).await {
                    internal.database.release(key.clone(), ctx.id).await;
                } else {
                    // Key not present.
                }
                internal.send(ctx.id, Packet::get(key)).await;
          
            },
            _ => unimplemented!()
        }
      
    }
    Ok(())
}

/// Handles watchng for a certain key.
async fn spawn_subscriber(
    key: Key,
    watcher: Arc<Watcher<WatchClient>>,
    internal: Arc<DriverInternal>,
    ctx: Arc<ClientContext>
) {
    loop {
        let val = watcher.wait().await.map(|f| (*f).to_owned());
        if watcher.is_killed() {
            // Break this and die.
            break;
        }
        internal.send(ctx.id, Packet::notify(key.clone(), val, false)).await;   
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use overseer::{
        access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}, network::decoder::Packet
    };
    use tokio::{net::{TcpSocket, TcpStream}, sync::{Barrier, Notify}};

    use crate::net::Driver;



    #[tokio::test]
    pub async fn test_client_subscription() {
        let mut server = Driver::start("127.0.0.1:0").await.unwrap();
        
        let mut staging = Arc::new(Barrier::new(2));
        let mut staging2 = Arc::new(Barrier::new(2));

        let handle = tokio::spawn({
            let port = server.port();
            let staging = Arc::clone(&staging);
            let staging2 = Arc::clone(&staging2);
            async move {
                let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;
                
                // Configure a lazy watch on the brokers key.
                Packet::watch("brokers", WatcherActivity::Lazy, WatcherBehaviour::Ordered).write(&mut connect).await?;
                
                tokio::spawn(async move { staging.wait().await; });

              

                // Let us wait until this gets inserted.
                if let Packet::Notify { key, value, .. } = Packet::read(&mut connect).await? {
                    assert_eq!(key.as_str(), "brokers");
                    assert_eq!(value.unwrap().as_integer().unwrap(), 145);
                } else {
                    panic!("Expected notify, received other type.");
                }

                // Let us wait until this gets updated.
                if let Packet::Notify { key, value, .. } = Packet::read(&mut connect).await? {
                    assert_eq!(key.as_str(), "brokers");
                    assert_eq!(value.unwrap().as_integer().unwrap(), 28);
                } else {
                    panic!("Expected notify, received other type.");
                }


                // Let us wait until this gets inserted.
                if let Packet::Notify { key, value, .. } = Packet::read(&mut connect).await? {
                    assert_eq!(key.as_str(), "brokers");
                    assert_eq!(value, None);
                } else {
                    panic!("Expected notify, received other type.");
                }


                // 

                Packet::release("brokers").write(&mut connect).await?;
                matches!(Packet::read(&mut connect).await?, Packet::Get { .. });


                staging2.wait().await;



                Ok::<(), NetworkError>(())
            }
        });

        let handle2 = tokio::spawn({
            let port = server.port();
            let staging = Arc::clone(&staging);
            async move {
                let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;
                

                staging.wait().await;


                // Configure a lazy watch on the brokers key.
                Packet::insert("brokers", 145).write(&mut connect).await?;
                Packet::insert("brokers", 28).write(&mut connect).await?;
                Packet::delete("brokers").write(&mut connect).await?;

                staging2.wait().await;

                Packet::insert("brokers", 13).write(&mut connect).await?;




                Ok::<(), NetworkError>(())
            }
        });

        let _ = handle.await.unwrap();
        let _ = handle2.await.unwrap();


    }

    #[tokio::test]
    pub async fn test_client_basic() {
        let mut server = Driver::start("127.0.0.1:0").await.unwrap();

        let handle = tokio::spawn({
            let port = server.port();
            async move {
                let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;


                Packet::Insert {
                    key: Key::from_str("hello"),
                    value: Value::Integer(62),
                }
                .write(&mut connect)
                .await?;

                let packet = Packet::read(&mut connect).await?;
                matches!(packet, Packet::Get { .. });

                Packet::Get { key: Key::from_str("hello") }.write(&mut connect).await?;

                if let Packet::GetReturn { key, value } = Packet::read(&mut connect).await? {
                    assert_eq!(key.as_str(), "hello");
                    assert_eq!(value.unwrap().as_integer().unwrap(), 62);
                } else {
                    panic!("Incorrect packet type.");
                }

                // Try deleting a key.
                Packet::delete(Key::from_str("hello")).write(&mut connect).await?;
                matches!(Packet::read(&mut connect).await?, Packet::Delete {..});

                // Get the key back, should be deleted.
                Packet::get(Key::from_str("hello")).write(&mut connect).await?;
                if let Packet::GetReturn { key, value } = Packet::read(&mut connect).await? {
                    assert_eq!(key.as_str(), "hello");
                    assert_eq!(value, None);
                } else {
                    panic!("Incorrect packet type.");
                }


                Ok::<(), NetworkError>(())
            }
        });

        let _ = handle.await.unwrap();


    }
}
