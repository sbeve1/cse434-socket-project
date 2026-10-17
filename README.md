# CSE 434 Socket Project — Group 57

A distributed hash table (DHT) implemented in Python using UDP sockets.
The manager tracks registered peers and DHT membership. Peers store storm
event records, forward management messages around a ring, and process
queries using the hot potato protocol.

## Requirements

- Python 3.10 or newer
- No external Python packages
- UDP connectivity between hosts
- Group 57 ports: 29500–29999

## Files

- `manager.py`: Peer registration and DHT management.
- `peer.py`: DHT construction, record storage, queries, joining, leaving,
  teardown, and deregistration.
- `prepare_data.py`: Extracts the 14 required fields from the NOAA dataset.
- `details-1996.csv`: Prepared dataset used for the full project.

## Dataset

The original storm event details dataset is available from NOAA:

https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

The full project uses the 1996 details file. To prepare it, place the
downloaded `.csv.gz` file beside `prepare_data.py`, update the script's
source filename if needed, and run:

```bash
python3 prepare_data.py
```

The resulting `details-1996.csv` must be in the working directory of
each peer, since a peer may become the leader during rebuilding.

## Running

Check the Python files:

```bash
python3 -m py_compile manager.py peer.py prepare_data.py
```

Start one manager:

```bash
python3 manager.py 29500
```

Start each peer in a separate terminal:

```bash
python3 peer.py <manager-ip> <manager-port>
```

For a local test:

```bash
python3 peer.py 127.0.0.1 29500
```

At the `peer>` prompt, register the peer:

```text
register Tevin 127.0.0.1 29501 29502
```

Use the peer host's reachable IPv4 address for distributed tests.
Give each peer a unique name and two unique ports: `m_port` for manager
communication and `p_port` for peer communication.

## Peer Commands

Enter these commands at the `peer>` prompt, using that process's
registered name.

| Command | Purpose |
| --- | --- |
| `register <name> <ip> <m_port> <p_port>` | Register with the manager. |
| `setup-dht <name> <n> <year>` | Build a DHT containing n peers. |
| `query-dht <name>` | Request a query, then enter the event ID when prompted. |
| `leave-dht <name>` | Leave the DHT and rebuild it with the remaining members. |
| `join-dht <name>` | Join the existing DHT and rebuild it. |
| `teardown-dht <name>` | Have the leader delete the DHT. |
| `deregister <name>` | Deregister a Free peer and exit its process. |

Commands such as `set-id`, `store`, `dht-complete`, and `dht-rebuilt`
are sent automatically by the programs.

## Full Project Demo Sequence

1. Start the manager and register six peers across at least four hosts.
2. Have one peer run `setup-dht <name> 5 1996`.
3. From the remaining Free peer, query event IDs `5536849`, `2402920`,
   `5539287`, and `55770111`.
4. Have a DHT member leave, then query from the departed peer.
5. Have one Free peer join, then query from the remaining Free peer.
6. Have the current leader run `teardown-dht <name>`.
7. Deregister all six peers, then stop the manager with Ctrl+C.

The selected ring members are randomized. Check the setup output to
identify the Free peer and the rebuild output to identify the current leader.

## Implementation Notes

Messages use UTF-8 JSON over UDP. Each peer uses separate manager and
peer sockets, with a background listener for peer messages.

Records use `EVENT_ID % table_size` to select a bucket and
`bucket % n` to select its owner. Bucket lists preserve records that
have the same hash position.

The implementation uses timeouts but does not retransmit lost messages.
If a management operation fails and leaves the DHT unfinished, restart
the manager and all peers before testing again.