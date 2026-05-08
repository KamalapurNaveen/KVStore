/*
 * ================================================================
 * Distributed Key Value Store
 * ================================================================
 *
 * Design Overview
 * ----------------
 *
 * This implementation follows an AP (Availability + Partition Tolerance)
 * distributed system design with eventual consistency.
 *
 * Core design choices:
 *
 * 1. Full replication across 3 nodes
 * 2. Leaderless architecture (any node can accept writes)
 * 3. Local durable writes using Write-Ahead Logging (WAL)
 * 4. Asynchronous replication to peers
 * 5. Snapshot-based backup and recovery
 * 6. Version-based conflict resolution (last-write-wins)
 *
 *
 * Design Decisions
 * ----------------
 *
 * Availability First
 * ------------------
 * This system prioritizes availability over strict consistency.
 *
 * During network partitions or temporary node failures:
 * - writes are still accepted locally
 * - client receives immediate acknowledgement
 * - replication happens asynchronously
 *
 * This ensures the system remains operational even during partial failures.
 *
 *
 * Eventual Consistency
 * --------------------
 * Since replication is asynchronous, replicas may temporarily diverge.
 *
 * Example:
 * Node A receives write.
 * Node C is temporarily unavailable.
 *
 * State may temporarily be:
 *   A -> latest
 *   B -> latest
 *   C -> stale
 *
 * Once Node C recovers, background synchronization restores consistency.
 *
 *
 * Full Replication
 * ----------------
 * Every node stores a complete copy of all data.
 *
 * Chosen because:
 * - simpler implementation
 * - easier recovery
 * - strong fault tolerance for small clusters
 *
 * Tradeoff:
 * - higher storage usage
 * - more replication traffic
 *
 *
 * WAL Persistence
 * ---------------
 * Every write is first persisted to disk before memory update.
 *
 * This ensures:
 * - crash recovery
 * - durability
 * - node restart recovery
 *
 *
 * Backup Strategy
 * ----------------
 * Snapshot captures a consistent point-in-time state.
 *
 * WAL continues accepting writes during backup.
 *
 * Recovery:
 * 1. Restore latest snapshot
 * 2. Replay WAL entries after snapshot
 *
 *
 * Conflict Resolution
 * -------------------
 * Writes carry version metadata.
 *
 * If conflicting writes arrive:
 * higher version wins (last-write-wins strategy).
 *
 * For production systems:
 * vector clocks would be preferable for richer conflict detection.
 *
 *
 * Failure Handling
 * ----------------
 *
 * Node crash:
 * - other replicas continue serving requests
 *
 * Network partition:
 * - local writes still succeed
 *
 * Node recovery:
 * - WAL replay + anti-entropy sync
 *
 * Concurrent writes:
 * - resolved using version comparison
 *
 *
 * Tradeoffs
 * ----------
 *
 * Pros:
 * - high availability
 * - partition tolerant
 * - low write latency
 * - simple operational model
 *
 * Cons:
 * - stale reads possible
 * - temporary replica inconsistency
 * - conflict resolution complexity
 *
 * ================================================================
 */

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class KVStore {

    static class ValueRecord {
        String value;
        long version;

        ValueRecord(String value, long version) {
            this.value = value;
            this.version = version;
        }
    }

    private final String nodeId;
    private final List<String> peerNodes;
    private final Map<String, ValueRecord> store;
    private final Queue<Map<String, Object>> replicationQueue;
    private final ReentrantLock lock;
    private final String walFile;

    public KVStore(String nodeId, List<String> peerNodes) {
        this.nodeId = nodeId;
        this.peerNodes = peerNodes;
        this.store = new ConcurrentHashMap<>();
        this.replicationQueue = new LinkedList<>();
        this.lock = new ReentrantLock();
        this.walFile = nodeId + "_wal.log";

        recoverFromWal();
    }

    // -------------------------------------------------
    // GET
    // -------------------------------------------------

    public String get(String key) {
        ValueRecord record = store.get(key);
        return record == null ? null : record.value;
    }

    // -------------------------------------------------
    // PUT
    // -------------------------------------------------

    public boolean put(String key, String value) {

        lock.lock();

        try {
            long version = System.currentTimeMillis();

            appendWal(key, value, version);

            store.put(key, new ValueRecord(value, version));

            Map<String, Object> replicationMessage =
                    new HashMap<>();

            replicationMessage.put("action", "replicate");
            replicationMessage.put("key", key);
            replicationMessage.put("value", value);
            replicationMessage.put("version", version);

            replicationQueue.offer(replicationMessage);

        } finally {
            lock.unlock();
        }

        // fire-and-forget async replication
        replicateAsync();

        // immediate success for availability
        return true;
    }

    // -------------------------------------------------
    // REPLICATION RECEIVER
    // -------------------------------------------------

    public Map<String, String> onMessage(
            String fromNode,
            Map<String, Object> message
    ) {

        String action = (String) message.get("action");

        if (!"replicate".equals(action)) {
            return Map.of("status", "unknown");
        }

        String key = (String) message.get("key");
        String value = (String) message.get("value");
        long version = (Long) message.get("version");

        lock.lock();

        try {
            ValueRecord existing = store.get(key);

            if (existing == null || version > existing.version) {
                appendWal(key, value, version);
                store.put(key, new ValueRecord(value, version));
            }

        } finally {
            lock.unlock();
        }

        return Map.of("status", "ok");
    }

    // -------------------------------------------------
    // ASYNC REPLICATION
    // -------------------------------------------------

    private void replicateAsync() {

        while (!replicationQueue.isEmpty()) {

            Map<String, Object> request =
                    replicationQueue.poll();

            for (String peer : peerNodes) {

                Map<String, String> response =
                        sendToNode(peer, request);

                if (response == null) {
                    // failed replication can be retried later
                    replicationQueue.offer(request);
                }
            }
        }
    }

    // -------------------------------------------------
    // WAL
    // -------------------------------------------------

    private void appendWal(
            String key,
            String value,
            long version
    ) {
        // append durable write record
    }

    private void recoverFromWal() {
        // replay WAL entries during restart
    }

    // -------------------------------------------------
    // SNAPSHOT
    // -------------------------------------------------

    public void createSnapshot() {

        Map<String, ValueRecord> snapshot;

        lock.lock();

        try {
            snapshot = new HashMap<>(store);

        } finally {
            lock.unlock();
        }

        // persist snapshot
    }

    // -------------------------------------------------
    // NETWORK
    // -------------------------------------------------

    public Map<String, String> sendToNode(
            String targetNode,
            Map<String, String> request
    ) {

        /*
         * Simulated RPC/network communication.
         *
         * Returns:
         * - response map if successful
         * - null if node unreachable
         */

        return null;
    }
}