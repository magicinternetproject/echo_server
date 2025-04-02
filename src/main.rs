use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("listening!");
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
	let (socket, _) = listener.accept().await.unwrap();
	// A new task is spawned for each inbound socket. The socket is
	// moved to the new task and processed there.
	let db = db.clone();

	tokio::spawn(async move {
	    process(socket, db).await;
	});
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};
    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
	let response = match Command::from_frame(frame).unwrap() {
	    Set(cmd) => {
		let mut db = db.lock().unwrap();
		db.insert(cmd.key().to_string(), cmd.value().clone());
		Frame::Simple("Ok".to_string())
	    }
	    Get(cmd) => {
		let db = db.lock().unwrap();
		if let Some(value) = db.get(cmd.key()) {
		    Frame::Bulk(value.clone())
		} else {
		    Frame::Null
		}
	    }
	    cmd => panic!("oh no you don't {:?}", cmd),
	};

	connection.write_frame(&response).await.unwrap();
    }
}
