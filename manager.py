import ipaddress
import json
import socket
import sys




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

    def main():
        if len(sys.argv) != 2:
            sys.exit("ERROR: Usage: python3 manager.py <port>")

        port = int(sys.argv[1])
        if not 29500 <= port <= 29999:
            sys.exit("ERROR: Group 57 must use ports 29500-29999")

        peers = {}

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
                    elif message.get("command") == "register":
                        response = register(message, peers, port)
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
                    
if __name__ == "main":
    main()