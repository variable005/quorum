#include <Arduino.h>
#include <esp_now.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>

#define NUM_NODES           8
#define HELLO_INTERVAL_MS   500
#define HEARTBEAT_MS        60
#define ELECTION_MIN_MS     200
#define ELECTION_MAX_MS     450
#define PEER_TIMEOUT_MS     1500
#define MAX_LOG_ENTRIES     64
#define MAX_PEERS           8
#define MSG_QUEUE_DEPTH     32

typedef enum : uint8_t {
  MSG_HELLO          = 0x01,
  MSG_REQUEST_VOTE   = 0x02,
  MSG_VOTE_GRANTED   = 0x03,
  MSG_VOTE_DENIED    = 0x04,
  MSG_APPEND_ENTRIES = 0x05,
  MSG_APPEND_REPLY   = 0x06,
} MsgType;

typedef enum : uint8_t {
  STATE_FOLLOWER  = 0,
  STATE_CANDIDATE = 1,
  STATE_LEADER    = 2,
} RaftState;

typedef struct {
  uint32_t term;
  uint32_t index;
  char     data[32];
} LogEntry;

typedef struct __attribute__((packed)) {
  MsgType  type;
  uint8_t  node_id;
  uint32_t term;
  uint32_t last_log_index;
  uint32_t last_log_term;
  uint32_t prev_log_index;
  uint32_t prev_log_term;
  uint32_t leader_commit;
  uint8_t  entry_count;
  LogEntry entries[2];
  uint16_t crc;
} RaftPacket;

typedef struct {
  uint8_t  mac[6];
  uint8_t  node_id;
  uint32_t last_seen_ms;
  bool     active;
  uint32_t next_index;
  uint32_t match_index;
} Peer;

typedef struct {
  uint8_t    sender_mac[6];
  RaftPacket pkt;
} InboundMsg;

static uint8_t           g_node_id;
static uint8_t           g_my_mac[6];
static RaftState         g_state          = STATE_FOLLOWER;
static uint32_t          g_current_term   = 0;
static uint8_t           g_voted_for_id   = 0xFF;
static uint8_t           g_votes_received = 0;
static uint8_t           g_leader_id      = 0xFF;

static LogEntry          g_log[MAX_LOG_ENTRIES];
static uint32_t          g_log_count      = 0;
static uint32_t          g_commit_index   = 0;
static uint32_t          g_last_applied   = 0;

static uint32_t          g_last_heartbeat_ms = 0;
static uint32_t          g_election_timeout  = 0;
static uint32_t          g_last_hello_ms     = 0;

static Peer              g_peers[MAX_PEERS];
static uint8_t           g_peer_count     = 0;
static SemaphoreHandle_t g_peer_mutex;
static QueueHandle_t     g_inbound_queue;
static volatile bool     g_killed = false;
static const uint8_t     BROADCAST_MAC[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};

static const char* state_name(RaftState s) {
  switch (s) {
    case STATE_FOLLOWER:  return "FOLLOWER";
    case STATE_CANDIDATE: return "CANDIDATE";
    case STATE_LEADER:    return "LEADER";
  }
  return "?";
}

static uint16_t compute_crc(const uint8_t* data, size_t len) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < len; i++) {
    crc ^= (uint16_t)data[i] << 8;
    for (int j = 0; j < 8; j++)
      crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : (crc << 1);
  }
  return crc;
}

static void stamp_crc(RaftPacket* p) {
  p->crc = 0;
  p->crc = compute_crc((uint8_t*)p, sizeof(RaftPacket));
}

static bool check_crc(const RaftPacket* p) {
  RaftPacket tmp = *p;
  tmp.crc = 0;
  return compute_crc((uint8_t*)&tmp, sizeof(RaftPacket)) == p->crc;
}

static uint32_t random_election_timeout() {
  return ELECTION_MIN_MS + (esp_random() % (ELECTION_MAX_MS - ELECTION_MIN_MS));
}

static void log_print(const char* fmt, ...) {
  char buf[192];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  Serial.printf("[N%d|T%lu|%s] %s\n", g_node_id, g_current_term, state_name(g_state), buf);
}

static void peer_upsert(const uint8_t* mac, uint8_t node_id) {
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (memcmp(g_peers[i].mac, mac, 6) == 0) {
      g_peers[i].last_seen_ms = millis();
      g_peers[i].active = true;
      xSemaphoreGive(g_peer_mutex);
      return;
    }
  }
  if (g_peer_count < MAX_PEERS) {
    memcpy(g_peers[g_peer_count].mac, mac, 6);
    g_peers[g_peer_count].node_id      = node_id;
    g_peers[g_peer_count].last_seen_ms = millis();
    g_peers[g_peer_count].active       = true;
    g_peers[g_peer_count].next_index   = g_log_count + 1;
    g_peers[g_peer_count].match_index  = 0;
    log_print("Discovered peer N%d", node_id);
    g_peer_count++;
  }
  xSemaphoreGive(g_peer_mutex);
}

static void peers_evict_stale() {
  uint32_t now = millis();
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (g_peers[i].active && (now - g_peers[i].last_seen_ms) > PEER_TIMEOUT_MS) {
      log_print("Peer N%d timed out", g_peers[i].node_id);
      g_peers[i].active = false;
    }
  }
  xSemaphoreGive(g_peer_mutex);
}

static int active_peer_count() {
  int c = 0;
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++)
    if (g_peers[i].active) c++;
  xSemaphoreGive(g_peer_mutex);
  return c;
}

static void espnow_send(const uint8_t* dest_mac, RaftPacket* pkt) {
  if (g_killed) return;
  stamp_crc(pkt);
  esp_now_send(dest_mac, (uint8_t*)pkt, sizeof(RaftPacket));
}

static void broadcast(RaftPacket* pkt) {
  if (g_killed) return;
  stamp_crc(pkt);
  esp_now_send(BROADCAST_MAC, (uint8_t*)pkt, sizeof(RaftPacket));
}

static void send_hello() {
  RaftPacket p = {};
  p.type    = MSG_HELLO;
  p.node_id = g_node_id;
  p.term    = g_current_term;
  broadcast(&p);
}

static void send_request_vote() {
  RaftPacket p = {};
  p.type           = MSG_REQUEST_VOTE;
  p.node_id        = g_node_id;
  p.term           = g_current_term;
  p.last_log_index = g_log_count;
  p.last_log_term  = g_log_count > 0 ? g_log[g_log_count-1].term : 0;
  broadcast(&p);
}

static void send_append_entries(Peer* peer) {
  RaftPacket p = {};
  p.type          = MSG_APPEND_ENTRIES;
  p.node_id       = g_node_id;
  p.term          = g_current_term;
  p.leader_commit = g_commit_index;
  uint32_t next = peer->next_index;
  if (next > 0 && next <= g_log_count) {
    p.prev_log_index = next - 1;
    p.prev_log_term  = (next >= 2) ? g_log[next-2].term : 0;
    uint8_t cnt = 0;
    for (uint32_t i = next; i <= g_log_count && cnt < 2; i++, cnt++) {
      p.entries[cnt] = g_log[i-1];
    }
    p.entry_count = cnt;
  } else {
    p.prev_log_index = g_log_count;
    p.prev_log_term  = g_log_count > 0 ? g_log[g_log_count-1].term : 0;
    p.entry_count    = 0;
  }
  espnow_send(peer->mac, &p);
}

static void send_heartbeats() {
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (g_peers[i].active) {
      send_append_entries(&g_peers[i]);
    }
  }
  xSemaphoreGive(g_peer_mutex);
}

static void become_follower(uint32_t term) {
  if (g_state != STATE_FOLLOWER || term > g_current_term) {
    log_print("-> FOLLOWER (term %lu)", term);
  }
  g_state             = STATE_FOLLOWER;
  g_current_term      = term;
  g_voted_for_id      = 0xFF;
  g_election_timeout  = random_election_timeout();
  g_last_heartbeat_ms = millis();
}

static void become_candidate() {
  g_state            = STATE_CANDIDATE;
  g_current_term++;
  g_voted_for_id     = g_node_id;
  g_votes_received   = 1;
  g_election_timeout = random_election_timeout();
  log_print("-> CANDIDATE term %lu (need %d votes)", g_current_term, (active_peer_count() + 2) / 2);
  send_request_vote();
}

static void become_leader() {
  g_state     = STATE_LEADER;
  g_leader_id = g_node_id;
  log_print("*** ELECTED LEADER term %lu cluster=%d ***", g_current_term, active_peer_count() + 1);
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    g_peers[i].next_index  = g_log_count + 1;
    g_peers[i].match_index = 0;
  }
  xSemaphoreGive(g_peer_mutex);
  send_heartbeats();
}

static void try_advance_commit() {
  for (uint32_t n = g_commit_index + 1; n <= g_log_count; n++) {
    if (g_log[n-1].term != g_current_term) continue;
    int matches = 1;
    int cluster = 0;
    xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
    for (int i = 0; i < g_peer_count; i++) {
      if (g_peers[i].active) {
        cluster++;
        if (g_peers[i].match_index >= n) matches++;
      }
    }
    xSemaphoreGive(g_peer_mutex);
    if (matches >= (cluster + 2) / 2) {
      g_commit_index = n;
      log_print("COMMITTED [%lu] \"%s\"", n, g_log[n-1].data);
    }
  }
}

static void apply_committed() {
  while (g_last_applied < g_commit_index) {
    g_last_applied++;
    log_print("APPLIED [%lu] \"%s\"", g_last_applied, g_log[g_last_applied-1].data);
  }
}

static void handle_hello(const uint8_t* mac, const RaftPacket* p) {
  peer_upsert(mac, p->node_id);
  if (p->term > g_current_term) become_follower(p->term);
}

static void handle_request_vote(const uint8_t* mac, const RaftPacket* p) {
  if (p->term < g_current_term) {
    RaftPacket r = {};
    r.type    = MSG_VOTE_DENIED;
    r.node_id = g_node_id;
    r.term    = g_current_term;
    espnow_send(mac, &r);
    return;
  }
  if (p->term > g_current_term) become_follower(p->term);

  bool log_ok = (p->last_log_term > (g_log_count > 0 ? g_log[g_log_count-1].term : 0)) ||
                (p->last_log_term == (g_log_count > 0 ? g_log[g_log_count-1].term : 0) &&
                 p->last_log_index >= g_log_count);

  RaftPacket r = {};
  r.node_id = g_node_id;
  r.term    = g_current_term;
  if ((g_voted_for_id == 0xFF || g_voted_for_id == p->node_id) && log_ok) {
    g_voted_for_id      = p->node_id;
    g_last_heartbeat_ms = millis();
    r.type              = MSG_VOTE_GRANTED;
    log_print("Voted for N%d", p->node_id);
  } else {
    r.type = MSG_VOTE_DENIED;
    log_print("Denied N%d (voted=%d)", p->node_id, g_voted_for_id);
  }
  espnow_send(mac, &r);
}

static void handle_vote_granted(const uint8_t* mac, const RaftPacket* p) {
  if (g_state != STATE_CANDIDATE || p->term != g_current_term) return;
  g_votes_received++;
  int majority = (active_peer_count() + 2) / 2;
  log_print("Vote from N%d -- %d/%d", p->node_id, g_votes_received, majority);
  if (g_votes_received >= majority) become_leader();
}

static void handle_vote_denied(const uint8_t* mac, const RaftPacket* p) {
  if (p->term > g_current_term) become_follower(p->term);
}

static void handle_append_entries(const uint8_t* mac, const RaftPacket* p) {
  peer_upsert(mac, p->node_id);

  RaftPacket r = {};
  r.type    = MSG_APPEND_REPLY;
  r.node_id = g_node_id;
  r.term    = g_current_term;

  if (p->term < g_current_term) {
    r.last_log_index = 0;
    espnow_send(mac, &r);
    return;
  }

  if (p->term > g_current_term) {
    become_follower(p->term);
  } else if (g_state != STATE_FOLLOWER) {
    become_follower(p->term);
  } else {
    g_last_heartbeat_ms = millis();
    g_leader_id = p->node_id;
  }

  if (p->prev_log_index > 0) {
    if (p->prev_log_index > g_log_count ||
        g_log[p->prev_log_index-1].term != p->prev_log_term) {
      r.last_log_index = g_log_count;
      espnow_send(mac, &r);
      return;
    }
  }

  for (int i = 0; i < p->entry_count; i++) {
    uint32_t idx = p->prev_log_index + 1 + i;
    if (idx <= g_log_count) {
      if (g_log[idx-1].term != p->entries[i].term) {
        g_log_count = idx - 1;
      } else {
        continue;
      }
    }
    if (g_log_count < MAX_LOG_ENTRIES) {
      g_log[g_log_count] = p->entries[i];
      g_log_count++;
      log_print("Appended [%lu] \"%s\"", g_log_count, p->entries[i].data);
    }
  }

  if (p->leader_commit > g_commit_index) {
    g_commit_index = min(p->leader_commit, g_log_count);
  }

  apply_committed();
  r.last_log_index = g_log_count;
  espnow_send(mac, &r);
}

static void handle_append_reply(const uint8_t* mac, const RaftPacket* p) {
  if (g_state != STATE_LEADER) return;
  if (p->term > g_current_term) {
    become_follower(p->term);
    return;
  }
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (memcmp(g_peers[i].mac, mac, 6) == 0) {
      if (p->last_log_index > 0) {
        g_peers[i].match_index = p->last_log_index;
        g_peers[i].next_index  = p->last_log_index + 1;
      } else {
        if (g_peers[i].next_index > 1) g_peers[i].next_index--;
      }
      break;
    }
  }
  xSemaphoreGive(g_peer_mutex);
  try_advance_commit();
  apply_committed();
}

static void on_data_recv(const esp_now_recv_info_t* info, const uint8_t* data, int len) {
  if (g_killed) return;
  if (len != sizeof(RaftPacket)) return;
  InboundMsg msg;
  memcpy(msg.sender_mac, info->src_addr, 6);
  memcpy(&msg.pkt, data, sizeof(RaftPacket));
  if (!check_crc(&msg.pkt)) return;
  xQueueSendFromISR(g_inbound_queue, &msg, NULL);
}

static void raft_task(void*) {
  g_election_timeout  = random_election_timeout();
  g_last_heartbeat_ms = millis();
  g_last_hello_ms     = millis();

  for (;;) {
    uint32_t now = millis();

    InboundMsg msg;
    while (xQueueReceive(g_inbound_queue, &msg, 0) == pdTRUE) {
      if (g_killed) continue;
      const RaftPacket* p = &msg.pkt;
      if (p->node_id == g_node_id) continue;
      switch (p->type) {
        case MSG_HELLO:          handle_hello(msg.sender_mac, p);          break;
        case MSG_REQUEST_VOTE:   handle_request_vote(msg.sender_mac, p);   break;
        case MSG_VOTE_GRANTED:   handle_vote_granted(msg.sender_mac, p);   break;
        case MSG_VOTE_DENIED:    handle_vote_denied(msg.sender_mac, p);    break;
        case MSG_APPEND_ENTRIES: handle_append_entries(msg.sender_mac, p); break;
        case MSG_APPEND_REPLY:   handle_append_reply(msg.sender_mac, p);   break;
      }
    }

    if (g_killed) {
      vTaskDelay(pdMS_TO_TICKS(100));
      continue;
    }

    if (now - g_last_hello_ms >= HELLO_INTERVAL_MS) {
      send_hello();
      peers_evict_stale();
      g_last_hello_ms = now;
    }

    if (g_state == STATE_LEADER && now - g_last_heartbeat_ms >= HEARTBEAT_MS) {
      send_heartbeats();
      g_last_heartbeat_ms = now;
    }

    if (g_state != STATE_LEADER && now - g_last_heartbeat_ms >= g_election_timeout) {
      if (active_peer_count() == 0) {
        g_last_heartbeat_ms = now;
      } else {
        log_print("Timeout %lums -- election!", g_election_timeout);
        become_candidate();
        g_last_heartbeat_ms = now;
      }
    }

    vTaskDelay(pdMS_TO_TICKS(5));
  }
}

static void propose_entry(const char* data) {
  if (g_state != STATE_LEADER) {
    Serial.printf("[N%d] Not leader -- leader is N%d\n", g_node_id, g_leader_id);
    return;
  }
  if (g_log_count >= MAX_LOG_ENTRIES) {
    Serial.println("Log full!");
    return;
  }
  LogEntry& e = g_log[g_log_count];
  e.term  = g_current_term;
  e.index = g_log_count + 1;
  strncpy(e.data, data, sizeof(e.data) - 1);
  e.data[sizeof(e.data)-1] = '\0';
  g_log_count++;
  log_print("Appended [%lu] \"%s\" replicating...", e.index, e.data);
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (g_peers[i].active) {
      g_peers[i].next_index = g_log_count;
      send_append_entries(&g_peers[i]);
    }
  }
  xSemaphoreGive(g_peer_mutex);
}

static void print_status() {
  Serial.println("================================");
  Serial.printf("Node %d | MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
    g_node_id,
    g_my_mac[0], g_my_mac[1], g_my_mac[2],
    g_my_mac[3], g_my_mac[4], g_my_mac[5]);
  Serial.printf("State: %s | Term: %lu | Killed: %s\n",
    state_name(g_state), g_current_term, g_killed ? "YES" : "no");
  Serial.printf("Leader: N%d | Voted: N%d\n",
    g_leader_id    == 0xFF ? 99 : g_leader_id,
    g_voted_for_id == 0xFF ? 99 : g_voted_for_id);
  Serial.printf("Log: %lu | Commit: %lu | Applied: %lu\n",
    g_log_count, g_commit_index, g_last_applied);
  xSemaphoreTake(g_peer_mutex, portMAX_DELAY);
  for (int i = 0; i < g_peer_count; i++) {
    if (g_peers[i].active) {
      Serial.printf("  Peer N%d next=%lu match=%lu\n",
        g_peers[i].node_id, g_peers[i].next_index, g_peers[i].match_index);
    }
  }
  xSemaphoreGive(g_peer_mutex);
  uint32_t start = g_log_count > 5 ? g_log_count - 5 : 0;
  for (uint32_t i = start; i < g_log_count; i++) {
    Serial.printf("  [%lu|T%lu] %s %s\n",
      g_log[i].index, g_log[i].term, g_log[i].data,
      i < g_commit_index ? "(committed)" : "(pending)");
  }
  Serial.println("================================");
}

void setup() {
  Serial.begin(115200);
  delay(500);

  WiFi.mode(WIFI_STA);
  esp_wifi_get_mac(WIFI_IF_STA, g_my_mac);
  g_node_id = g_my_mac[5] % NUM_NODES;

  Serial.printf("\n=== RAFT NODE %d BOOTING ===\n", g_node_id);
  Serial.printf("MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
    g_my_mac[0], g_my_mac[1], g_my_mac[2],
    g_my_mac[3], g_my_mac[4], g_my_mac[5]);
  Serial.println("Commands: CMD:<text> | STATUS | KILL | REVIVE");

  if (esp_now_init() != ESP_OK) {
    Serial.println("FATAL: ESP-NOW init failed");
    ESP.restart();
  }

  esp_now_peer_info_t peer_info = {};
  memcpy(peer_info.peer_addr, BROADCAST_MAC, 6);
  peer_info.channel = 0;
  peer_info.encrypt = false;
  esp_now_add_peer(&peer_info);
  esp_now_register_recv_cb(on_data_recv);

  g_peer_mutex    = xSemaphoreCreateMutex();
  g_inbound_queue = xQueueCreate(MSG_QUEUE_DEPTH, sizeof(InboundMsg));

  xTaskCreatePinnedToCore(raft_task, "raft", 8192, NULL, 5, NULL, 1);
  Serial.println("Raft running on Core 1. Discovering peers...");
}

void loop() {
  if (Serial.available()) {
    String line = Serial.readStringUntil('\n');
    line.trim();
    if (line.startsWith("CMD:")) {
      propose_entry(line.substring(4).c_str());
    } else if (line == "STATUS") {
      print_status();
    } else if (line == "KILL") {
      g_killed = true;
      Serial.printf("[N%d] *** KILLED ***\n", g_node_id);
    } else if (line == "REVIVE") {
      g_killed = false;
      become_follower(g_current_term);
      Serial.printf("[N%d] Revived\n", g_node_id);
    } else if (line.length() > 0) {
      Serial.println("Commands: CMD:<text> | STATUS | KILL | REVIVE");
    }
  }
  delay(10);
}
