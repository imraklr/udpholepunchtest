use std::{
    env,
    io,
    net::{SocketAddr, UdpSocket},
    thread,
    time::Duration,
};

const BUF_SIZE: usize = 1024;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!(
            "Usage:\n\
             \n\
             Server:\n\
             cargo run -- server 0.0.0.0:9000\n\
             \n\
             Peer:\n\
             cargo run -- peer A <server-ip:port>\n\
             cargo run -- peer B <server-ip:port>"
        );
        return Ok(());
    }

    match args[1].as_str() {
        "server" => {
            let addr = args
                .get(2)
                .map(String::as_str)
                .unwrap_or("0.0.0.0:9000");

            run_server(addr)
        }

        "peer" => {
            if args.len() != 4 {
                eprintln!(
                    "Usage: cargo run -- peer <A|B> <server-ip:port>"
                );
                return Ok(());
            }

            let name = &args[2];
            let server: SocketAddr = args[3].parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid server address: {e}"),
                )
            })?;

            run_peer(name, server)
        }

        _ => {
            eprintln!("Unknown command.");
            Ok(())
        }
    }
}

// ------------------------------------------------------------
// Rendezvous server
// ------------------------------------------------------------

fn run_server(bind_addr: &str) -> io::Result<()> {
    let socket = UdpSocket::bind(bind_addr)?;

    println!("Rendezvous server listening on {bind_addr}");

    let mut peer_a: Option<SocketAddr> = None;
    let mut peer_b: Option<SocketAddr> = None;

    let mut buf = [0u8; BUF_SIZE];

    loop {
        let (len, source) = socket.recv_from(&mut buf)?;

        let message = String::from_utf8_lossy(&buf[..len])
            .trim()
            .to_string();

        println!("Received from {source}: {message}");

        match message.as_str() {
            "REGISTER A" => {
                peer_a = Some(source);

                println!("Peer A registered as {source}");

                // Tell A about B if B is already registered.
                if let Some(b) = peer_b {
                    let msg = format!("PEER {b}");
                    socket.send_to(msg.as_bytes(), source)?;

                    // Also tell B about A.
                    let msg = format!("PEER {source}");
                    socket.send_to(msg.as_bytes(), b)?;
                }
            }

            "REGISTER B" => {
                peer_b = Some(source);

                println!("Peer B registered as {source}");

                // Tell B about A if A is already registered.
                if let Some(a) = peer_a {
                    let msg = format!("PEER {a}");
                    socket.send_to(msg.as_bytes(), source)?;

                    // Also tell A about B.
                    let msg = format!("PEER {source}");
                    socket.send_to(msg.as_bytes(), a)?;
                }
            }

            _ => {
                println!("Unknown message from {source}: {message}");
            }
        }
    }
}

// ------------------------------------------------------------
// Peer
// ------------------------------------------------------------

fn run_peer(name: &str, server: SocketAddr) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;

    socket.set_read_timeout(Some(Duration::from_millis(500)))?;

    let local_addr = socket.local_addr()?;

    println!("Peer {name}");
    println!("Local UDP address: {local_addr}");
    println!("Rendezvous server: {server}");

    // Register with the rendezvous server.
    let registration = format!("REGISTER {name}");

    socket.send_to(registration.as_bytes(), server)?;

    println!("Registered with rendezvous server.");

    // Wait for the server to tell us the other peer's
    // public endpoint.
    let peer_addr = loop {
        let mut buf = [0u8; BUF_SIZE];

        match socket.recv_from(&mut buf) {
            Ok((len, source)) => {
                let message = String::from_utf8_lossy(&buf[..len])
                    .trim()
                    .to_string();

                println!("Received from {source}: {message}");

                if let Some(addr) = message.strip_prefix("PEER ") {
                    match addr.parse::<SocketAddr>() {
                        Ok(addr) => break addr,
                        Err(e) => {
                            eprintln!("Invalid peer address: {e}");
                        }
                    }
                }
            }

            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                // Re-register periodically in case the first packet
                // was lost.
                socket.send_to(registration.as_bytes(), server)?;
            }

            Err(e) => return Err(e),
        }
    };

    println!("Other peer appears to be at {peer_addr}");

    println!();
    println!("Starting UDP hole punching...");
    println!("Local socket: {local_addr}");
    println!("Remote socket: {peer_addr}");
    println!();

    // We need to continuously send packets toward the peer.
    //
    // The first packets cause the local NAT to create an outbound
    // UDP mapping. Simultaneously, the other peer does the same.
    //
    // Once both NATs have mappings allowing the packets through,
    // communication can become bidirectional.

    socket.set_read_timeout(Some(Duration::from_millis(100)))?;

    let receive_socket = socket.try_clone()?;
    let peer_addr_for_thread = peer_addr;

    let receiver = thread::spawn(move || {
        let mut buf = [0u8; BUF_SIZE];

        loop {
            match receive_socket.recv_from(&mut buf) {
                Ok((len, source)) => {
                    let message =
                        String::from_utf8_lossy(&buf[..len]);

                    println!(
                        "\n<<< Received from {source}: {message}"
                    );
                }

                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    // Nothing received.
                }

                Err(e) => {
                    eprintln!("Receive error: {e}");
                    break;
                }
            }
        }
    });

    // Send punching packets.
    for i in 1..=60 {
        let message = format!(
            "HELLO from peer {name} packet={i}"
        );

        match socket.send_to(message.as_bytes(), peer_addr_for_thread) {
            Ok(bytes) => {
                println!(
                    ">>> Sent {bytes} bytes to {peer_addr_for_thread}"
                );
            }

            Err(e) => {
                eprintln!("Send error: {e}");
            }
        }

        thread::sleep(Duration::from_millis(500));
    }

    println!();
    println!("Hole-punching test finished.");

    // Keep the peer alive so you can observe late packets.
    println!("Listening for another 30 seconds...");

    thread::sleep(Duration::from_secs(30));

    drop(receiver);

    Ok(())
}
