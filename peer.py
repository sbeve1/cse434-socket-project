import json
import socket
import sys
import threading
import csv
import queue

def send_peer(p_socket, address, message):
    """encode and send a peer message"""
    data = json.dumps(message).encode("utf-8")
    p_socket.sendto(data, address)
    print(f"SEND to peer {address}: {message}", flush=True)


def send_right(p_socket, state, message):
    """forward a message to the next peer in the ring"""
    name, ip, port = state["right"]
    send_peer(p_socket, (ip, port), message)


def wait_for_reply(state, command, key, value):
    """wait for the listener to deliver the expected confirmation"""
    while True:
        try:
            message = state["replies"].get(timeout=5)
        except queue.Empty:
            raise TimeoutError(f"No confirmation for {command}: {value}")

        if message["command"] == command and message.get(key) == value:
            return message

def configure_ring(state, peer_id, ring, table_size):
    """save the peer's id, ring members, and its right neighbor"""
    state["id"] = peer_id
    state["n"] = len(ring)
    state["ring"] = ring
    state["right"] = ring[(peer_id + 1) % len(ring)]
    state["table_size"] = table_size
    state["table"] = [[] for _ in range(table_size)] # each position has a lits to handle hash collisions
    state["count"] = 0

    print(
        f"\nRing configured: ID={peer_id}, "
        f"size={state['n']}, right neighbor={state['right'][0]}",
        flush=True
    )

def store_local(state, pos, record):
    """store a record without overwriting other records at the same position"""
    bucket = state["table"][pos]

    # don't count the same event if it arrived already
    for existing in bucket:
        if existing["EVENT_ID"] == record["EVENT_ID"]:
            return

    bucket.append(record)
    state["count"] += 1

    print(
        f"STORED event={record['EVENT_ID']} "
        f"at peer={state['id']}, position={pos}",
        flush=True,
    )

def listen_to_peers(p_socket, state):
    """handle peer messages while the main thread accepts commands"""
    while True:
        try:
            data, sender = p_socket.recvfrom(65535)
        except OSError:
            return

        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print("\nERROR: invalid peer message", flush=True)
            continue

        if not isinstance(message, dict):
            print("\nERROR: expected json object", flush=True)
            continue

        print(f"\nRECV from peer {sender}: {message}", flush=True)
        command = message.get("command")

        if command == "set-id":
            configure_ring(
                state, 
                message["id"], 
                message["peers"],
                message["table_size"]
            )

            # confirm the config by forwarding to the right eventually reaching the leader
            reply = {
                "command": "ready",
                "id": state["id"]
            }
            send_right(p_socket, state, reply)

        elif command in ("ready", "stored"):
            if state["id"] == 0:
                state["replies"].put(message)
            else:
                send_right(p_socket, state, message)

        elif command == "store":
            record = message["record"]
            event_id = int(record["EVENT_ID"])
            pos = event_id % state["table_size"]
            owner = pos % state["n"]

            if owner == state["id"]:
                store_local(state, pos, record)

                reply = {
                    "command": "stored",
                    "event_id": event_id
                }
                send_right(p_socket, state, reply)

            elif state["id"] == 0:
                print("ERROR: Unstored record returned to leader", flush=True)

            else:
                send_right(p_socket, state, message)

        elif command == "collect-counts":
            if state["id"] == 0:
                state["replies"].put(message)
            else:
                message["counts"].append({
                    "id": state["id"],
                    "count": state["count"]
                })
                send_right(p_socket, state, message)

        else:
            print("ERROR: unknown peer command", flush=True)

def build_ring(p_socket, state, ring, table_size):
    """configure all the peers before record distribution"""
    configure_ring(state, 0, ring)


    # working backward to ensure right is already configured
    for peer_id in range(len(ring) - 1, 0, -1):
        peer_name, peer_ip, peer_port = ring[peer_id]

        message = {
            "command": "set-id",
            "id": peer_id,
            "n": len(ring),
            "peers": ring,
            "table_size": table_size
        }     

        send_peer(p_socket, (peer_ip, peer_port), message)
        wait_for_reply(state, "ready", "id", peer_id)

    print("All peers confirmed ring config", flush=True)

def distribute_records(p_socket, state, records):
    """store each record at its owner, using the ring for forwarding."""
    expected_counts = [0] * state["n"]

    for record in records:
        event_id = int(record["EVENT_ID"])
        pos = event_id % state["table_size"]
        owner = pos % state["n"]
        expected_counts[owner] += 1

        if owner == state["id"]:
            store_local(state, pos, record)
        else:
            message = {
                "command": "store",
                "record": record,
            }
            send_right(p_socket, state, message)
            wait_for_reply(state, "stored", "event_id", event_id)

    # collect the actual counts only after every record has been confirmed as stored
    message = {
        "command": "collect-counts",
        "counts": [{"id": state["id"], "count": state["count"]}],
    }
    send_right(p_socket, state, message)
    result = wait_for_reply(
        state, "collect-counts", "command", "collect-counts"
    )

    actual_counts = [0] * state["n"]
    for entry in result["counts"]:
        actual_counts[entry["id"]] = entry["count"]

    if actual_counts != expected_counts:
        raise ValueError(
            f"Record counts do not match: "
            f"expected={expected_counts}, actual={actual_counts}"
        )

    print("\nDHT record counts:", flush=True)
    for peer_id, count in enumerate(actual_counts):
        peer_name = state["ring"][peer_id][0]
        print(f"Peer {peer_id} ({peer_name}): {count} records", flush=True)

    print(f"Total stored: {sum(actual_counts)}", flush=True)

def next_prime(number):
    """Helper to return the first prime strictly greater than the number"""
    candidate = number + 1

    while True:
        is_prime = candidate >= 2

        divisor = 2
        while divisor * divisor <= candidate:
            if candidate % divisor == 0:
                is_prime = False
                break
            divisor += 1

        if is_prime:
            return candidate

        candidate += 1

def load_records(year):
    """Read the csv while keeping the fields of the records"""
    filename = f"details-{year}.csv"

    with open(filename, encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        records = list(reader)

    table_size = next_prime(2 * len(records))
    return records, table_size

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
                "right": None,
                "replies": queue.Queue()
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

                    try:
                        records, table_size = load_records(year)
                    except (OSError, csv.Error) as error:
                        print(f"ERROR: Could not read dataset: {error}")
                        continue

                    print(
                        f"Loaded {len(records)} records; "
                        f"hash table size={table_size}",
                        flush=True,
                    )

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

                        try:
                            build_ring(p_socket, state, ring, table_size)
                            distribute_records(p_socket, state, records)

                            message = {
                                "command": "dht-complete",
                                "name": name,
                            }
                            data = json.dumps(message).encode("utf-8")
                            m_socket.sendto(data, manager_address)
                            print(
                                f"SEND to manager: {message}",
                                flush=True,
                            )

                            data, sender = m_socket.recvfrom(65535)
                            response = json.loads(data.decode("utf-8"))
                            print(
                                f"RECV from {sender}: {response}",
                                flush=True,
                            )

                            if response["status"] == "SUCCESS":
                                print("DHT setup complete", flush=True)
                            else:
                                print("ERROR: Manager rejected completion")

                        except (OSError, ValueError) as error:
                            print(f"ERROR: DHT setup stopped: {error}")
                            print("Restart all programs before retrying")

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
                    
