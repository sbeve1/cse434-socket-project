import json
import socket
import sys
import threading
import csv
import queue
import random

def send_peer(p_socket, address, message):
    """encode and send a peer message"""
    data = json.dumps(message).encode("utf-8")
    p_socket.sendto(data, address)
    print(f"SEND to peer {address}: {message}", flush=True)


def send_right(p_socket, state, message):
    """forward a message to the next peer in the ring"""
    name, ip, port = state["right"]
    send_peer(p_socket, (ip, port), message)


def wait_for_reply(state, command, key, value, timeout=5):
    """wait for the listener to deliver the expected confirmation"""
    while True:
        try:
            message = state["replies"].get(timeout=timeout)
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
    # each position has a list to handle hash collisions
    state["table"] = [[] for _ in range(table_size)]
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

def find_event(p_socket, state, message):
    """search locally, then pass the query to a random unvisited peer"""
    event_id = message["event_id"]
    pos = event_id % state["table_size"]

    visited = message["id_seq"]
    visited.append(state["id"])

    record = None
    for entry in state["table"][pos]:
        if int(entry["EVENT_ID"]) == event_id:
            record = entry
            break

    remaining = []
    for peer_id in range(state["n"]):
        if peer_id not in visited:
            remaining.append(peer_id)

    if record is not None or not remaining:
        reply = {
            "command": "find-event-result",
            "event_id": event_id,
            "status": "SUCCESS" if record is not None else "FAILURE",
            "id_seq": visited
        }

        if record is not None:
            reply["record"] = record

        requester_name, requester_ip, requester_port = message["requester"]
        send_peer(p_socket, (requester_ip, requester_port), reply)
    else:
        next_id = random.choice(remaining)
        next_name, next_ip, next_port = state["ring"][next_id]
        send_peer(p_socket, (next_ip, next_port), message)

def delete_local_table(state):
    """delete stored records while keeping the ring information"""
    state["table"] = []
    state["count"] = 0
    print(f"Deleted local hash table at peer {state['id']}", flush=True)


def teardown_ring(p_socket, state):
    """send teardown around the ring and wait for it to return"""
    message = {
        "command": "teardown",
        "origin": state["id"]
    }

    send_right(p_socket, state, message)
    wait_for_reply(state, "teardown", "origin", state["id"])

def rebuild_records(p_socket, state, message):
    """reload the dataset and tell the departing peer when finished"""
    requester_name, requester_ip, requester_port = message["requester"]

    try:
        records, table_size = load_records(message["year"])

        if table_size != state["table_size"]:
            raise ValueError("Dataset size differs from the original setup")

        distribute_records(p_socket, state, records)

        reply = {
            "command": "rebuild-complete",
            "status": "SUCCESS"
        }
    except (OSError, ValueError, csv.Error) as error:
        reply = {
            "command": "rebuild-complete",
            "status": "FAILURE",
            "reason": str(error)
        }

    send_peer(p_socket, (requester_ip, requester_port), reply)


def leave_ring(p_socket, state, year):
    """remove this peer and rebuild with its right neighbor as leader"""
    old_id = state["id"]
    old_ring = state["ring"]
    requester = old_ring[old_id]

    new_ring = old_ring[old_id + 1:] + old_ring[:old_id]
    new_leader = new_ring[0]

    teardown_ring(p_socket, state)

    message = {
        "command": "reset-id",
        "origin": requester,
        "next_id": 0,
        "peers": new_ring,
        "table_size": state["table_size"]
    }

    send_right(p_socket, state, message)
    wait_for_reply(state, "reset-complete", "command", "reset-complete")

    message = {
        "command": "rebuild-dht",
        "year": year,
        "requester": requester
    }

    send_peer(p_socket, (new_leader[1], new_leader[2]), message)

    result = wait_for_reply(
        state, "rebuild-complete", "command", "rebuild-complete",
        timeout=120
    )

    if result["status"] != "SUCCESS":
        raise ValueError(result["reason"])

    state["id"] = None
    state["n"] = 0
    state["ring"] = []
    state["right"] = None
    state["table_size"] = 0

    return new_leader[0]

def join_ring(p_socket, state, message):
    """add a peer at the end of the ring and rebuild the DHT"""
    requester = message["requester"]

    try:
        records, table_size = load_records(message["year"])

        if table_size != state["table_size"]:
            raise ValueError("Dataset size differs from the original setup")

        old_right = state["right"]
        new_ring = state["ring"] + [requester]

        teardown_ring(p_socket, state)
        configure_ring(state, 0, new_ring, table_size)

        # configure the new peer, whose right neighbor is the leader
        new_id = len(new_ring) - 1
        setup = {
            "command": "set-id",
            "id": new_id,
            "n": len(new_ring),
            "peers": new_ring,
            "table_size": table_size
        }

        send_peer(p_socket, (requester[1], requester[2]), setup)
        wait_for_reply(state, "ready", "id", new_id)

        # update existing peers by following the old ring
        reset = {
            "command": "reset-id",
            "origin": new_ring[0],
            "next_id": 1,
            "peers": new_ring,
            "table_size": table_size
        }

        send_peer(p_socket, (old_right[1], old_right[2]), reset)
        wait_for_reply(state, "reset-complete", "command", "reset-complete")

        distribute_records(p_socket, state, records)

        reply = {
            "command": "join-complete",
            "status": "SUCCESS"
        }

    except (OSError, ValueError, csv.Error) as error:
        reply = {
            "command": "join-complete",
            "status": "FAILURE",
            "reason": str(error)
        }

    send_peer(p_socket, (requester[1], requester[2]), reply)

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
            print("\nInvalid peer message", flush=True)
            continue

        if not isinstance(message, dict):
            print("\nExpected json object", flush=True)
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
                print("Unstored record returned to leader", flush=True)

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

        elif command == "find-event":
            find_event(p_socket, state, message)

        elif command == "find-event-result":
            state["replies"].put(message)

        elif command == "teardown":
            delete_local_table(state)

            if state["id"] == message["origin"]:
                state["replies"].put(message)
            else:
                send_right(p_socket, state, message)

        elif command == "reset-id":
            own_peer = state["ring"][state["id"]]

            if own_peer[0] == message["origin"][0]:
                state["replies"].put({
                    "command": "reset-complete"
                })
            else:
                # forward using the OLD ring so the message returns
                # to the departing peer even after neighbors change
                old_right = state["right"]

                configure_ring(
                    state,
                    message["next_id"],
                    message["peers"],
                    message["table_size"]
                )

                message["next_id"] += 1
                send_peer(
                    p_socket,
                    (old_right[1], old_right[2]),
                    message
                )

        elif command == "rebuild-dht":
            # keep the listener available for storage confirmations
            worker = threading.Thread(
                target=rebuild_records,
                args=(p_socket, state, message),
                daemon=True
            )
            worker.start()

        elif command == "rebuild-complete":
            state["replies"].put(message)

        elif command == "join-ring":
            worker = threading.Thread(
                target=join_ring,
                args=(p_socket, state, message),
                daemon=True
            )
            worker.start()

        elif command == "join-complete":
            state["replies"].put(message)

        else:
            print("Unknown peer command", flush=True)

def build_ring(p_socket, state, ring, table_size):
    """configure all the peers before record distribution"""
    configure_ring(state, 0, ring, table_size)


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
        sys.exit("Use python3 peer.py <manager-ip> <manager-port>")

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

                if command == "join-dht":
                    if not registered:
                        print("You need to register first")
                        continue

                    if len(parts) != 2:
                        print("Use join-dht <peer-name>")
                        continue

                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    message = {
                        "command": "join-dht",
                        "name": name
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("No response from the manager")
                        continue

                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] != "SUCCESS":
                        print(response.get("reason", "Join denied"))
                        continue

                    leader_name, leader_ip, leader_port = response["leader"]

                    message = {
                        "command": "join-ring",
                        "requester": [name, ip, p_port],
                        "year": response["year"]
                    }

                    try:
                        send_peer(
                            p_socket, (leader_ip, leader_port), message
                        )

                        result = wait_for_reply(
                            state, "join-complete", "command", "join-complete",
                            timeout=120
                        )

                        if result["status"] != "SUCCESS":
                            raise ValueError(result["reason"])

                        message = {
                            "command": "dht-rebuilt",
                            "name": name,
                            "new_leader": leader_name
                        }

                        data = json.dumps(message).encode("utf-8")
                        m_socket.sendto(data, manager_address)
                        print(f"SEND to manager: {message}", flush=True)

                        data, sender = m_socket.recvfrom(65535)
                        response = json.loads(data.decode("utf-8"))
                        print(f"RECV from {sender}: {response}", flush=True)

                        if response["status"] == "SUCCESS":
                            print(
                                f"Joined DHT. Leader: {leader_name}",
                                flush=True
                            )
                        else:
                            print(response.get("reason", "Completion denied"))

                    except (OSError, ValueError) as error:
                        print(f"Joining DHT stopped: {error}")
                        print("Restart all programs before retrying")

                    continue

                if command == "leave-dht":
                    if not registered:
                        print("You need to register first")
                        continue

                    if len(parts) != 2:
                        print("Use leave-dht <peer-name>")
                        continue

                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    message = {
                        "command": "leave-dht",
                        "name": name
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("No response from the manager")
                        continue

                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] != "SUCCESS":
                        print(response.get("reason", "Leave denied"))
                        continue

                    try:
                        new_leader = leave_ring(
                            p_socket, state, response["year"]
                        )

                        message = {
                            "command": "dht-rebuilt",
                            "name": name,
                            "new_leader": new_leader
                        }

                        data = json.dumps(message).encode("utf-8")
                        m_socket.sendto(data, manager_address)
                        print(f"SEND to manager: {message}", flush=True)

                        data, sender = m_socket.recvfrom(65535)
                        response = json.loads(data.decode("utf-8"))
                        print(f"RECV from {sender}: {response}", flush=True)

                        if response["status"] == "SUCCESS":
                            print(
                                f"Left DHT. New leader: {new_leader}",
                                flush=True
                            )
                        else:
                            print(response.get("reason", "Completion denied"))

                    except (OSError, ValueError) as error:
                        print(f"Leaving DHT stopped: {error}")
                        print("Restart all programs before retrying")

                    continue

                if command == "teardown-dht":
                    if not registered:
                        print("You need to register first")
                        continue

                    if len(parts) != 2:
                        print("Use teardown-dht <peer-name>")
                        continue

                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    message = {
                        "command": "teardown-dht",
                        "name": name
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("No response from the manager")
                        continue

                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] != "SUCCESS":
                        print(response.get("reason", "Teardown denied"))
                        continue

                    try:
                        teardown_ring(p_socket, state)

                        message = {
                            "command": "teardown-complete",
                            "name": name
                        }

                        data = json.dumps(message).encode("utf-8")
                        m_socket.sendto(data, manager_address)
                        print(f"SEND to manager: {message}", flush=True)

                        data, sender = m_socket.recvfrom(65535)
                        response = json.loads(data.decode("utf-8"))
                        print(f"RECV from {sender}: {response}", flush=True)

                        if response["status"] == "SUCCESS":
                            print("DHT teardown complete", flush=True)
                        else:
                            print(response.get("reason", "Completion denied"))

                    except OSError as error:
                        print(f"DHT teardown stopped: {error}")
                        print("Restart all programs before retrying")

                    continue

                if command == "deregister":
                    if not registered:
                        print("You need to register first")
                        continue

                    if len(parts) != 2:
                        print("Use deregister <peer-name>")
                        continue

                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    message = {
                        "command": "deregister",
                        "name": name
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("No response from the manager")
                        continue

                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] == "SUCCESS":
                        print(f"{name} deregistered. Exiting.", flush=True)
                        return

                    print(response.get("reason", "Deregistration denied"))
                    continue

                if command == "query-dht":
                    if not registered:
                        print("You need to register first")
                        continue

                    if len(parts) != 2:
                        print("Use query-dht <peer-name>")
                        continue

                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    message = {
                        "command": "query-dht",
                        "name": name
                    }

                    data = json.dumps(message).encode("utf-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except socket.timeout:
                        print("No response from the manager")
                        continue

                    response = json.loads(data.decode("utf-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] != "SUCCESS":
                        print(response.get("reason", "Query denied"))
                        continue

                    try:
                        event_id = int(input("Event ID: "))
                    except ValueError:
                        print("Event ID must be an integer")
                        continue

                    start_name, start_ip, start_port = response["peer"]

                    message = {
                        "command": "find-event",
                        "event_id": event_id,
                        "requester": [name, ip, p_port],
                        "id_seq": []
                    }

                    send_peer(p_socket, (start_ip, start_port), message)

                    try:
                        result = wait_for_reply(
                            state, "find-event-result", "event_id", event_id
                        )
                    except TimeoutError as error:
                        print(error)
                        continue

                    if result["status"] == "SUCCESS":
                        for field, value in result["record"].items():
                            print(f"{field}: {value}")
                    else:
                        print(f"Storm event {event_id} not found in the DHT.")

                    print(f"Visited peer IDs: {result['id_seq']}")
                    continue

                if command == "setup-dht":
                    if not registered:
                        print("You need to register first")
                        continue
                    if len(parts) != 4:
                        print("Use setup-dht <peer-name> <n> <year>")
                        continue
                    if parts[1] != name:
                        print("Use this peer's registered name")
                        continue

                    try:
                        n = int(parts[2])
                        year = int(parts[3])
                    except ValueError:
                        print("Size and year must be integers")
                        continue

                    try:
                        records, table_size = load_records(year)
                    except (OSError, csv.Error) as error:
                        print(f"Could not read dataset: {error}")
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
                        print("No response from the manager")
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
                                print("Manager rejected completion")

                        except (OSError, ValueError) as error:
                            print(f"DHT setup stopped: {error}")
                            print("Restart all programs before retrying")

                    continue

                if command != "register":
                    print("Unknown command")
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
                    print("No response. check manager")
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
                    print("Registration failed. correct the values")
                    return

if __name__ == "__main__":
    main()
                    
