import ipaddress
import json
import socket
import sys
import random




def register(message, peers, manager_port): 
    """validate a registration before adding the peer"""
    name = message.get("name")
    ip = message.get("ip")
    m_port = message.get("m_port")
    p_port = message.get("p_port")

    if (
        not isinstance(name, str)
        or not name.isascii()
        or not name.isalpha()
        or not 1 <= len(name) <= 15
    ):
        return {
            "status": "FAILURE", 
            "reason": "Invalid peer name"
        }

    try:
        ipaddress.IPv4Address(ip)
    except (ipaddress.AddressValueError, TypeError):
        return {"status": "FAILURE", "reason": "Invalid IPv4 address"}

    # port range for socket project group 57
    if any((type(port) is not int or not 29500 <= port <= 29999) 
           for port in (m_port, p_port)):
        return {
            "status": "FAILURE", 
            "reason": "Invalid port"
        }

    if name in peers:
        return {
            "status": "FAILURE", 
            "reason": "Name has already been registered"
        }

    used_ports = {manager_port}
    for peer in peers.values():
        used_ports.update((peer["m_port"], peer["p_port"]))

    if m_port == p_port or m_port in used_ports or p_port in used_ports:
        return {
            "status": "FAILURE", 
            "reason": "Ports must be unique"
        }

    peers[name] = {
        "ip": ip,
        "m_port": m_port,
        "p_port": p_port,
        "state": "Free"
    }
    return {"status": "SUCCESS"}

def setup_dht(message, peers, dht):
    """select peers that will construct the DHT"""
    name = message.get("name")
    n = message.get("n")
    year = message.get("year")

    if not isinstance(name, str) or name not in peers:
        return {
            "status": "FAILURE",
            "reason": "peer is not registered"
        }
    if not isinstance(n, int) or n < 3:
        return {
            "status": "FAILURE",
            "reason": "DHT needs >= 3 peers"
        }
    if not isinstance(year, int) or not 1950 <= year <= 2019:
        return {
            "status": "FAILURE",
            "reason": "dataset has invalid year"
        }
    if dht["status"] != "None":
        return {
            "status": "FAILURE",
            "reason": "a DHT already exists"
        }
    if peers[name]["state"] != "Free":
        return {
            "status": "FAILURE",
            "reason": "the leader must be free"
        }

    # collect eligible peers, excluding the leader
    available = []
    for peer_name, peer in peers.items():
        if peer_name != name and peer["state"] == "Free":
            available.append(peer_name)

    if len(available) < n - 1:
        return {
            "status": "FAILURE",
            "reason": "not enough free peers"
        }

    # select different peers randomly and put the leader as first
    selected = [name] + random.sample(available, n - 1)

    peers[name]["state"] = "Leader"
    for peer_name in selected[1:]:
        peers[peer_name]["state"] = "InDHT"

    dht["status"] = "Building"
    dht["leader"] = name
    dht["members"] = selected
    dht["year"] = year

    # each entry contains the name, ip, and the peer-communication port number
    ring = []
    for peer_name in selected:
        peer = peers[peer_name]
        ring.append((peer_name, peer["ip"], peer["p_port"]))

    return {
        "status": "SUCCESS",
        "peers": ring
    }    

def dht_complete(message, dht):
    """construction is marked complete when the leader says so"""
    if dht["status"] != "Building":
        return {
            "status": "FAILURE",
            "reason": "no DHT is being built"
        }
    if message.get("name") != dht["leader"]:
        return {
            "status": "FAILURE",
            "reason": "only the leader can complete the setup"
        }

    dht["status"] = "Ready"
    return {
        "status": "SUCCESS"
    }
    
def query_dht(message, peers, dht):
    """give a free peer a random DHT member to contact"""
    name = message.get("name")

    if dht["status"] != "Ready":
        return {
            "status": "FAILURE",
            "reason": "DHT is not ready"
        }

    if peers[name]["state"] != "Free":
        return {
            "status": "FAILURE",
            "reason": "only a Free peer can query the DHT"
        }

    selected = random.choice(dht["members"])
    peer = peers[selected]

    return {
        "status": "SUCCESS",
        "peer": [selected, peer["ip"], peer["p_port"]]
    }
    

def main():
    if len(sys.argv) != 2:
        sys.exit("Use python3 manager.py <port>")

    port = int(sys.argv[1])
    if not 29500 <= port <= 29999:
        sys.exit("Group 57 must use ports 29500-29999")

    peers = {}

    dht = {
        "status": "None",
        "leader": None,
        "members": [],
        "year": None
    }

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("0.0.0.0", port))
        print(f"Manager listening on UDP port {port}", flush=True)

        while True:
            data, sender = sock.recvfrom(65535)
            print(f"RECV from {sender}: {data}", flush=True)

            try:
                message = json.loads(data.decode("utf-8"))

                if not isinstance(message, dict):
                    response = {
                        "status": "FAILURE",
                        "reason": "Expected JSON object"
                    }
                else:
                    command = message.get("command")
                    name = message.get("name")

                    # during dht construction, only completion is permitted
                    if dht["status"] == "Building" and command != "dht-complete":
                        response = {
                            "status": "FAILURE",
                            "reason": "Waiting for dht-complete"
                        }
                    elif command == "register":
                        response = register(message, peers, port)
                    elif command in ("setup-dht", "dht-complete", "query-dht"):
                        # check that the request is from a named peer
                        if not isinstance(name, str) or name not in peers:
                            response = {
                                "status": "FAILURE",
                                "reason": "peer is not registered"
                            }
                        elif sender != (peers[name]["ip"], peers[name]["m_port"]):
                            response = {
                                "status": "FAILURE",
                                "reason": "sender does not match registered peer"
                            }
                        elif command == "setup-dht":
                            response = setup_dht(message, peers, dht)
                        elif command == "query-dht":
                            response = query_dht(message, peers, dht)
                        else:
                            response = dht_complete(message, dht)

                    else:
                        response = {
                            "status": "FAILURE",
                            "reason": "This command isn't supported"
                    }
                        
            except (UnicodeDecodeError, json.JSONDecodeError):
                response = {
                    "status": "FAILURE",
                    "reason": "Invalid JSON"
                }

            sock.sendto(json.dumps(response).encode("utf-8"), sender)
            print(f"SEND to {sender}: {response}", flush=True)
            print(f"Registered peers: {peers}", flush=True)
            print(f"DHT: {dht}", flush=True)
                
if __name__ == "__main__":
    main()