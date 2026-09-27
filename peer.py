import json
import socket
import sys



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

                while True:
                    parts = input("peer> ").split()

                    command = parts[0]

                    if not parts:
                        continue

                    if len(parts) != 5:
                        print("use register <name> <ip> <m-port> <p-port>")
                        continue

                    if command != "register":
                        print("only register is implemented so far")
                        continue

                    if registered:
                        print("this peer is already registered")
                        continue

                    name = parts[1]
                    ip = parts[2]

                    try:
                        m_port = parts[3]
                        p_port = parts[4]
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

                    data = json.dumps(message).encode("utr-8")
                    m_socket.sendto(data, manager_address)
                    print(f"SEND to manager: {message}", flush=True)

                    # wait a bit for the manager before accepting another command
                    m_socket.settimeout(5)

                    try:
                        data, sender = m_socket.recvfrom(65535)
                    except sockte.timeout:
                        print("ERROR: no response. check manager")
                        return

                    response = json.loads(data.decode("utr-8"))
                    print(f"RECV from {sender}: {response}", flush=True)

                    if response["status"] == "SUCCESS":
                        registered = True
                        print(f"{name} registered successfully")
                    else:
                        print("ERROR: registration failed. correct the values")
                        return

if __name__ == "__main__":
    main()
                    
