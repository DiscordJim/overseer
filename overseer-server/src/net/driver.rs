use std::{net::ToSocketAddrs, path::Path, rc::Rc, sync::Arc};

use dashmap::DashMap;
use overseer::{error::NetworkError, models::Key, network::{OverseerSerde, Packet, PacketId, PacketPayload}};
use tokio::{net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpListener, TcpStream}, sync::mpsc::{Receiver, Sender}};


use crate::database::{Database, WatchClient, Watcher};

pub struct Driver {
    internal: Rc<DriverInternal>
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
    write_queue: DashMap<ClientId, Sender<Packet<'static>>>
}

impl DriverInternal {
    pub async fn send(&self, id: ClientId, packet: Packet<'static>) {
        let queue = self.write_queue.get(&id).unwrap().value().clone();
        queue.send(packet).await.unwrap();
    }
}

impl Driver {
    pub async fn start<A, P, S>(addr: A, path: P, name: S) -> Result<Self, NetworkError>
    where 
        A: tokio::net::ToSocketAddrs,
        P: AsRef<Path>,
        S: AsRef<str>
    {
        let internal = Rc::new(DriverInternal {
            database: Database::new(path, name).await?,
            stream: TcpListener::bind(addr).await?,
            write_queue: DashMap::new(),
        });

        monoio::spawn(accept_connection_loop(Rc::clone(&internal)));

        Ok(Self {
            internal: Rc::clone(&internal),
        })
    }
    pub fn port(&self) -> u16 {
        self.internal.stream.local_addr().unwrap().port()
    }
}

async fn accept_connection_loop(internal: Rc<DriverInternal>) -> Result<(), NetworkError> {
    let mut counter = 0;
    loop {
        let (sock, _) = internal.stream.accept().await?;
        handle_client(sock, ClientId(counter), Rc::clone(&internal)).await;
        counter += 1;
    }
}

async fn handle_client(
    socket: TcpStream,
    id: ClientId,
    internal: Rc<DriverInternal>,
) {
    println!("Spawning new client...");
    let (read, write) = socket.into_split();
    let (sender, receiver) = tokio::sync::mpsc::channel(250);
    internal.write_queue.insert(id, sender);
    let ctx = Rc::new(ClientContext {
        id,
        watches: DashMap::new(),
    });
    monoio::spawn(handle_client_write(write, receiver));
    monoio::spawn(handle_client_read(read, internal, ctx));
}

struct ClientContext {
    id: ClientId,
    watches: DashMap<Key, Rc<Watcher<WatchClient>>>,
}

async fn handle_client_write(
    mut socket: OwnedWriteHalf,
    mut receiver: Receiver<Packet<'static>>,
) -> Result<(), NetworkError> {
    loop {
        let packet = receiver.recv().await.unwrap();
        packet.serialize(&mut socket).await?;
    }
}

async fn handle_client_read(
    mut socket: OwnedReadHalf,
    internal: Rc<DriverInternal>,
    ctx: Rc<ClientContext>,
) -> Result<(), NetworkError> {
    loop {
        let packet = Packet::deserialize(&mut socket).await?;
        let packet_id = packet.id();
        match packet.into_payload() {
            PacketPayload::Insert { key, value } => {
                internal.database.insert(key.clone(), (*value).clone()).await?;
                internal.send(ctx.id, Packet::vreturn(packet_id, &*key, Some(&*value)).to_owned()).await;
            }
            PacketPayload::Get { key } => {
                // let key = &**key;
                let value = internal.database.get(&*key).await;
                internal
                    .send(ctx.id, Packet::vreturn(packet_id, &*key, value.as_deref()).to_owned())
                    .await;
            }
            PacketPayload::Delete { key } => {
                internal.database.delete(&*key).await?;
                internal.send(ctx.id, Packet::get(packet_id, &*key).to_owned()).await;
            }
            PacketPayload::Watch {
                key,
                activity,
                behaviour,
            } => {
                let wow = Rc::new(
                    internal
                        .database
                        .subscribe(key.clone(), ctx.id, behaviour, activity)
                        .await?,
                );
                ctx.watches.insert((*key).clone(), Rc::clone(&wow));
                
                monoio::spawn({
                    let internal = Rc::clone(&internal);
                    let ctx = Rc::clone(&ctx);
                    let key = key.clone();
                    async move {
                        spawn_subscriber(&*key, wow, internal, ctx).await;
                    }
                });
                internal.send(ctx.id, Packet::get(packet_id, &*key).to_owned()).await;
            }
            PacketPayload::Release { key } => {
                if let Some(..) = ctx.watches.remove(&key) {
                    internal.database.release(key.clone(), ctx.id).await?;
                } else {
                    // Key not present.
                }
                internal.send(ctx.id, Packet::get(packet_id, &*key).to_owned()).await;
            }
            _ => unimplemented!(),
        }
    }
}

/// Handles watchng for a certain key.
async fn spawn_subscriber(
    key: &Key,
    watcher: Rc<Watcher<WatchClient>>,
    internal: Rc<DriverInternal>,
    ctx: Rc<ClientContext>,
) {
    loop {
        let val = watcher.wait().await;
        if watcher.is_killed() {
            // Break this and die.
            break;
        }
        internal
            .send(ctx.id, Packet::notify(PacketId::zero(), key, val.as_deref(), false).to_owned())
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, rc::Rc, sync::Arc};

    use overseer::{
        access::{WatcherActivity, WatcherBehaviour},
        error::NetworkError,
        models::{Key, Value}, network::{Packet, PacketId, PacketPayload}
    };
    use monoio::{
        net::TcpStream,
    };
    use tokio::sync::Barrier;

    use crate::net::Driver;

    // #[monoio::test]
    // pub async fn test_client_subscription() {
    //     let td = tempfile::tempdir().unwrap();
    //     let server = Driver::start("127.0.0.1:0", td.path(), "db").await.unwrap();

    //     let staging = Rc::new(Barrier::new(2));
    //     let staging2 = Rc::new(Barrier::new(2));

    //     let handle = monoio::spawn({
    //         let port = server.port();
    //         let staging = Rc::clone(&staging);
    //         let staging2 = Rc::clone(&staging2);
    //         async move {
    //             let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;

    //             // Configure a lazy watch on the brokers key.
    //             Packet::watch(PacketId::zero(), Key::from_str("brokers"), WatcherActivity::Lazy, WatcherBehaviour::Ordered)
    //                 .write(&mut connect)
    //                 .await?;
    //             // Let us wait until this gets inserted.
    //             if let PacketPayload::Get { key, .. } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "brokers");
    //                 // assert_eq!(value.as_ref().unwrap().as_integer().unwrap(), 145);
    //             } else {
    //                 panic!("Expected notify, received other type.");
    //             }

    //             monoio::spawn(async move {
    //                 staging.wait().await;
    //             });

    //             // Let us wait until this gets inserted.
    //             if let PacketPayload::Notify { key, value, .. } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "brokers");
    //                 assert_eq!(value.as_ref().unwrap().as_integer().unwrap(), 145);
    //             } else {
    //                 panic!("Expected notify, received other type.");
    //             }

    //             // Let us wait until this gets updated.
    //             if let PacketPayload::Notify { key, value, .. } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "brokers");
    //                 assert_eq!(value.as_ref().unwrap().as_integer().unwrap(), 28);
    //             } else {
    //                 panic!("Expected notify, received other type.");
    //             }

    //             // Let us wait until this gets inserted.
    //             if let PacketPayload::Notify { key, value, .. } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "brokers");
    //                 assert_eq!(*value, None);
    //             } else {
    //                 panic!("Expected notify, received other type.");
    //             }

    //             //

    //             Packet::release(PacketId::zero(), Key::from_str("brokers")).write(&mut connect).await?;
    //             matches!(Packet::read(&mut connect).await?.payload(), PacketPayload::Get { .. });

    //             staging2.wait().await;

    //             Ok::<(), NetworkError>(())
    //         }
    //     });

    //     let handle2 = monoio::spawn({
    //         let port = server.port();
    //         let staging = Arc::clone(&staging);
    //         async move {
    //             let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;

    //             staging.wait().await;

    //             // Configure a lazy watch on the brokers key.
    //             Packet::insert(PacketId::zero(), Key::from_str("brokers"), Value::Integer(145)).write(&mut connect).await?;
    //             Packet::insert(PacketId::zero(), Key::from_str("brokers"), Value::Integer(28)).write(&mut connect).await?;
    //             Packet::delete(PacketId::zero(), Key::from_str("brokers")).write(&mut connect).await?;

    //             staging2.wait().await;

    //             Packet::insert(PacketId::zero(), Key::from_str("brokers"), Value::Integer(13)).write(&mut connect).await?;

    //             Ok::<(), NetworkError>(())
    //         }
    //     });

    //     handle.await.unwrap().unwrap();
    //     handle2.await.unwrap().unwrap();
    // }

    // #[monoio::test]
    // pub async fn test_client_basic() {
    //     let td = tempfile::tempdir().unwrap();
    //     let server = Driver::start("127.0.0.1:0", td.path(), "db").await.unwrap();

    //     let handle = monoio::spawn({
    //         let port = server.port();
    //         async move {
    //             let mut connect = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), port)).await?;

    //             Packet::insert(PacketId::zero(), Key::from_str("hello"), Value::Integer(62)).write(&mut connect).await?;
    //             // Packet::Insert {
    //             //     key: Key::from_str("hello"),
    //             //     value: Value::Integer(62),
    //             // }
    //             // .write(&mut connect)
    //             // .await?;

    //             match Packet::read(&mut connect).await?.payload() {
    //                 PacketPayload::Return { .. } => {},
    //                 packet => { panic!("Expected a return packet but received packet {:?}", packet) }
    //             }
                

    //             // matches!(packet, Packet::Get { .. });

    //             // Packet::Get {
    //             //     key: Key::from_str("hello"),
    //             // }
    //             // .write(&mut connect)
    //             // .await?;

    //             Packet::get(PacketId::zero(), Key::from_str("hello")).write(&mut connect).await?;

    //             if let PacketPayload::Return { key, value } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "hello");
    //                 assert_eq!(value.as_ref().unwrap().as_integer().unwrap(), 62);
    //             } else {
    //                 panic!("Incorrect packet type.");
    //             }

    //             // Try deleting a key.
    //             Packet::delete(PacketId::zero(), Key::from_str("hello"))
    //                 .write(&mut connect)
    //                 .await?;
    //             match Packet::read(&mut connect).await?.payload() {
    //                 PacketPayload::Get { .. } => {},
    //                 packet => { panic!("Expected a get packet but received packet {:?}", packet) }
    //             }
               

    //             // Get the key back, should be deleted.
    //             Packet::get(PacketId::zero(), Key::from_str("hello"))
    //                 .write(&mut connect)
    //                 .await?;
    //             if let PacketPayload::Return { key, value } = Packet::read(&mut connect).await?.payload() {
    //                 assert_eq!(key.as_str(), "hello");
    //                 assert_eq!(*value, None);
    //             } else {
    //                 panic!("Incorrect packet type.");
    //             }

    //             Ok::<(), NetworkError>(())
    //         }
    //     });

    //     handle.await.unwrap().unwrap();
    // }
}
