import json
import socket
import sys
import threading



def configure_ring(state, peer_id, ring):
    """save the peer's id, ring members, and its right neighbor"""
    state["id"] = peer_id
    state["n"] = len(ring)
    state["ring"] = ring

    right_id = (peer_id + 1) % len(ring)
    state["right"] = ring[right_id]

    print(
        f"\nRing configured: ID={peer_id}, "
        f"size={state['n']}, right neighbor={state['right'][0]}",
        flush=True
    )

def listen_to_peers(p_socket, state):
    """now receive peer messages while the main thread accepts concurrent commands"""
    while True:
        try:
            data, sender = p_socket.recvfrom(65535)
        except OSError:
            return

        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print("\nERROR: invalid peer message")
            continue

        if not isinstance(message, dict):
            print("\nERROR: expected json object")
            continue

        print(f"\nRECV from peer {sender}: {message}", flush=True)

        if message.get("command") == "set-id":
            configure_ring(state, message["id"], message["peers"])
        else:
            print("ERROR: unknown peer command", flush=True)

def build_ring(p_socket, state, ring):
    """configure the leader and send ids to the other peers"""
    configure_ring(state, 0, ring)

    for peer_id in range(1, len(ring)):
        peer_name, peer_ip, peer_port = ring[peer_id]

        message = {
            "command": "set-id",
            "id": peer_id,
            "n": len(ring),
            "peers": ring
        }     

        data = json.dumps(message).encode("utf-8")
        p_socket.sendto(data, (peer_ip, peer_port))
        print(f"SEND to {peer_name}: {message}", flush=True)

def main():
    if len(sys.argv) != 3:
        sys.exit("ERROR: Use python3 peer.py <manager-ip> <manager-port>")

    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_address = (manager_ip, manager_port)

    # peers have separate sockets for manager and peer communicatoin
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as m_socket:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as p_socket:
            registered = False

            state = {
                "id": None,
                "n": 0,
                "ring": [],
                "right": None
            }

            while True:
                parts = input("peer> ").split()

                if not parts:
                    continue

                command = parts[0]

                if command == "setup-dht":
                    if not registered:
                        print("ERROR: you need to register first")
                        continue
                    if len(parts) != 4:
                        print("ERROR: use setup-dht <peer-name> <n> <year>")
                        continue
                    if parts[1] != name:
                        print("ERROR: use this peer's registered name")
                        continue

                    try:
                        n = int(parts[2])
                        year = int(parts[3])
                    except ValueError:
                        print("ERROR: size and year must be integers")
                        continue

                    message = {
                        "command": "setup-dht",
                        "name": name,
                        "n": n,
                        "year": year
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)
    
                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("ERROR: no response from the manager")
                        continue
    
                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)
    
                    if response["status"] == "SUCCESS":
                        ring = response["peers"]
                        print(f"selected ring members: {ring}")
                        build_ring(p_socket, state, ring)

                    continue

                if command != "register":
                    print("ERROR: unknown command")
                    continue

                if len(parts) != 5:
                    print("use register <name> <ip> <m-port> <p-port>")
                    continue

                if registered:
                    print("this peer is already registered")
                    continue

                name = parts[1]
                ip = parts[2]

                try:
                    m_port = int(parts[3])
                    p_port = int(parts[4])
                except ValueError:
                    print("ports must be integers")
                    continue

                if not (29500 <= m_port <= 29999
                        and 29500 <= p_port <= 29999):
                    print("use ports 29500-29999")
                    continue

                if m_port == p_port:
                    print("the two ports muts be different")
                    continue

                try:
                    m_socket.bind((ip, m_port))
                    p_socket.bind((ip, p_port))
                except OSError as error:
                    print(f"couldn't bind the sockets sry : {error}")
                    return

                message = {
                    "command": "register",
                    "name": name, 
                    "ip": ip,
                    "m_port": m_port,
                    "p_port": p_port
                }

                data = json.dumps(message).encode("utf-8")
                m_socket.sendto(data, manager_address)
                print(f"SEND to manager: {message}", flush=True)

                # wait a bit for the manager before accepting another command
                m_socket.settimeout(5)

                try:
                    data, sender = m_socket.recvfrom(65535)
                except socket.timeout:
                    print("ERROR: no response. check manager")
                    return

                response = json.loads(data.decode("utf-8"))
                print(f"RECV from {sender}: {response}", flush=True)

                if response["status"] == "SUCCESS":
                    registered = True
                    print(f"{name} registered successfully")

                    listener = threading.Thread(
                        target=listen_to_peers,
                        args=(p_socket, state),
                        daemon=True
                    )
                    listener.start()
                else:
                    print("ERROR: registration failed. correct the values")
                    return

if __name__ == "__main__":
    main()
                    
