# quorum

> A full implementation of the Raft distributed consensus algorithm running across 8 ESP32 nodes communicating over raw ESP-NOW frames. No libraries. No coordinator. No single point of failure.

![Demo](demo.png)

---

## what is this

**Quorum** is a bare-metal distributed systems project that implements the [Raft consensus algorithm](https://raft.github.io/) on a mesh of 8 ESP32 microcontrollers. Every node runs identical firmware. They boot knowing nothing about each other, discover the mesh autonomously via gossip, elect a leader, replicate a shared log, and recover from failures — all without any central server, broker, or orchestrator.

This is not a sensor project. There are no LEDs. There is no display. The entire point is what happens invisibly between the chips over the air.

---

## why this exists

Most ESP32 projects use the chip as a dumb WiFi client — connect to a router, send data to a server, done. That's fine. But the ESP32 has two cores, a hardware random number generator, and ESP-NOW: a connectionless peer-to-peer protocol that bypasses the entire WiFi stack and lets nodes talk directly to each other at the MAC layer.

That combination is enough to run real distributed systems primitives. Not simulated. Not abstracted. The actual algorithms that keep distributed databases consistent — running on $5 microcontrollers sitting on a desk.

The specific problem Raft solves is **consensus**: how do multiple nodes agree on a sequence of values when any of them can fail at any time, messages can be delayed, and there is no god's-eye view of the system? This is one of the hardest problems in computer science. It is also the problem that keeps etcd, CockroachDB, Consul, and TiKV consistent in production at companies like Google, Cloudflare, and PingCAP.

Quorum is that algorithm, on chips you can hold in your hand.

---

## the algorithm

Raft divides consensus into three relatively independent subproblems:

### leader election

Every node starts as a **Follower**. Each follower has an election timeout — a randomized timer between 200ms and 450ms — that resets every time it hears from a valid leader. If the timer expires without a heartbeat, the follower becomes a **Candidate**, increments its term number, votes for itself, and broadcasts a `RequestVote` RPC to all peers.

A candidate wins the election if it receives votes from a **majority** of the cluster. That majority requirement — the quorum — is what gives this project its name. A cluster of 8 nodes requires 5 votes to elect a leader. You cannot have two leaders simultaneously because two nodes cannot each independently hold a majority of 8 votes.

The randomized timeouts are the key insight: they make it statistically unlikely that two nodes start an election at the exact same time. In practice, one node almost always wins cleanly on the first ballot.

Once elected, the leader immediately begins sending **heartbeats** — empty `AppendEntries` RPCs — to all followers every 60ms. This suppresses further elections for as long as the leader is alive.

### log replication

The replicated log is the actual output of consensus. When a client proposes a value, the leader appends it to its own log with the current term number, then fires `AppendEntries` to all followers in parallel. Each follower appends the entry if (and only if) its own log is consistent with the leader's — specifically, if `prevLogIndex` and `prevLogTerm` match. If they don't, the follower rejects the entry and the leader backtracks its `nextIndex` pointer for that peer until it finds the point of divergence.

An entry is considered **committed** only after the leader has received acknowledgements from a majority of nodes. The leader then advances its `commitIndex` and notifies followers of the new commit boundary in subsequent heartbeats. Followers apply committed entries to their state machine in order.

This design guarantees that a committed entry will survive any future leader election — because any node that wins a future election must have received votes from a majority, and at least one member of that majority held the committed entry.

### fault tolerance

If a follower stops receiving heartbeats — because the leader crashed, was partitioned, or was deliberately killed — its election timeout fires. It becomes a candidate and starts a new election. The cluster elects a new leader within one election timeout window (200–450ms). The new leader begins replicating from the point where the old leader left off.

When the old leader comes back online, it finds that the cluster has moved to a higher term. Its own `AppendEntries` calls are rejected. It steps down to follower, adopts the new term, and catches up its log via the standard replication mechanism. No special recovery path. No manual intervention.

---

## how it was built

### transport layer

ESP-NOW operates at the 802.11 MAC layer. Frames are delivered directly between MAC addresses with no TCP, no IP, no routing, no association. Maximum payload is 250 bytes. Latency is in the single-digit milliseconds.

Every Raft message is packed into a single `RaftPacket` struct, stamped with a CRC-16/CCITT checksum, and sent either unicast to a specific peer or broadcast to `FF:FF:FF:FF:FF:FF`. Corrupt frames are silently dropped at the receiver before they enter the state machine.

```c
typedef struct __attribute__((packed)) {
  MsgType  type;           // HELLO / REQUEST_VOTE / VOTE_GRANTED / APPEND_ENTRIES / APPEND_REPLY
  uint8_t  node_id;
  uint32_t term;
  uint32_t last_log_index;
  uint32_t last_log_term;
  uint32_t prev_log_index;
  uint32_t prev_log_term;
  uint32_t leader_commit;
  uint8_t  entry_count;
  LogEntry entries[2];     // max 2 entries per packet, fits in 250 byte limit
  uint16_t crc;
} RaftPacket;
```

### discovery

Nodes have no hardcoded peer list. On boot, each node begins broadcasting a `HELLO` frame every 500ms containing its MAC address and node ID. Any node that receives a `HELLO` it hasn't seen before adds the sender to its peer table. Peers that go silent for more than 1500ms are evicted. The peer table is live and dynamic for the entire runtime of the firmware.

### dual-core architecture

The ESP32 has two Xtensa LX6 cores. The ESP-NOW receive callback fires on **Core 0**, which is also where the WiFi stack lives. Running Raft logic directly inside that callback would create race conditions and violate ESP-IDF's constraints on callback duration.

Instead, the callback does exactly one thing: it validates the CRC and enqueues the packet into a FreeRTOS queue. The entire Raft state machine runs on **Core 1**, pinned via `xTaskCreatePinnedToCore`. Core 1 drains the queue, processes messages, and drives all state transitions. A mutex protects the peer table which both cores access.

```
Core 0 (WiFi/ESP-NOW)          Core 1 (Raft state machine)
─────────────────────          ───────────────────────────
on_data_recv()                 raft_task()
  └─ validate CRC                └─ drain inbound queue
  └─ xQueueSendFromISR()         └─ process messages
                                 └─ fire timers
                                 └─ drive elections
                                 └─ replicate log
```

### randomness

Election timeouts are generated using `esp_random()`, which reads from the ESP32's hardware random number generator seeded by thermal noise from the RF subsystem. This is genuine entropy — not `rand()`, not a software PRNG. Two nodes will never have the same election timeout window.

---

## message flow

### normal operation (no failures)

```
Node A          Node B          Node C          Node D
  │               │               │               │
  │── HELLO ─────►│               │               │   discovery phase
  │── HELLO ──────────────────────►               │
  │               │                               │
  │  [election timeout fires on A]                │
  │               │               │               │
  │── REQUEST_VOTE ───────────────────────────────►
  │◄─ VOTE_GRANTED ───────────────────────────────│
  │◄─ VOTE_GRANTED ────────────────►              │
  │                                               │
  │  [majority reached — A becomes LEADER]        │
  │                                               │
  │── APPEND_ENTRIES (heartbeat) ─────────────────►  every 60ms
  │◄─ APPEND_REPLY ───────────────────────────────│
```

### leader failure and re-election

```
[LEADER A crashes]

Node B          Node C          Node D          Node E
  │               │               │               │
  │  [no heartbeat — election timeout fires on B] │
  │               │               │               │
  │── REQUEST_VOTE ───────────────────────────────►
  │◄─ VOTE_GRANTED ───────────────────────────────│
  │◄─ VOTE_GRANTED ────────────────►              │
  │                                               │
  │  [B elected new leader, term+1]               │
  │                                               │
  │── APPEND_ENTRIES ─────────────────────────────►  replication resumes

[LEADER A comes back online]

  A receives APPEND_ENTRIES from B with higher term
  A steps down to FOLLOWER
  A catches up log via standard replication
```

---

## numbers

| parameter | value |
|---|---|
| nodes | 8 |
| heartbeat interval | 60ms |
| election timeout | 200–450ms (randomized) |
| failure detection | < 450ms |
| re-election time | < 450ms |
| max log entries | 64 |
| packet size | ~130 bytes (well within ESP-NOW 250B limit) |
| transport | ESP-NOW (802.11 MAC layer) |
| entropy source | ESP32 hardware RNG |
| RTOS | FreeRTOS (ESP-IDF) |
| cores used | 2 (pinned tasks) |

---

## what quorum is not

Quorum does not persist the log to flash. A power cycle loses state. This is intentional — the focus is the consensus mechanism itself, not storage. Adding NVS-backed persistence is a straightforward extension.

Quorum does not implement log compaction or snapshotting. With `MAX_LOG_ENTRIES` set to 64, a long-running cluster will eventually fill its log. Again, intentional scope decision.

Quorum does not implement cluster membership changes. The node count is fixed at compile time. Dynamic membership (adding/removing nodes while the cluster runs) is a known-hard extension to Raft described in the original paper.

---

## further reading

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) — Diego Ongaro & John Ousterhout, the original Raft paper
- [The Raft Consensus Algorithm](https://raft.github.io/) — interactive visualization
- [ESP-NOW Programming Guide](https://docs.espressif.com/projects/esp-idf/en/latest/esp32/api-reference/network/esp_now.html) — Espressif IDF docs
- [FreeRTOS Task Notifications](https://www.freertos.org/RTOS-task-notifications.html)

---

*a project by Hariom sharnam*
