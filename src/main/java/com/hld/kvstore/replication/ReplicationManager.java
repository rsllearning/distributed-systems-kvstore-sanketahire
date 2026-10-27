package com.hld.kvstore.replication;

import com.hld.kvstore.node.ClusterNode;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TODO (Assignment §3.3 — Replication). This is one of the 4 required
 * TODOs for the one-day scope — but only the SYNCHRONOUS branch is
 * required.
 *
 * Why this matters:
 *   - In a replicated cluster, the leader or primary node must copy writes
 *     to follower nodes so they all keep a consistent view of data.
 *   - Synchronous replication means the write does not return to the caller
 *     until every follower has received and applied it.
 *   - This is important for correctness when the system later reads from
 *     follower replicas or when tests verify that follower state is updated.
 *
 * REQUIRED — SYNCHRONOUS: replicate(...) must not return until every
 * follower has acknowledged the write (call follower.put(...) on the
 * calling thread, in a loop, log a ReplicationLogEntry per follower with
 * the observed latency, and only return after all followers are done).
 *
 * STRETCH GOAL — ASYNCHRONOUS: replicate(...) should return immediately
 * after kicking off the follower writes on asyncExecutor; the caller does
 * not wait for followers to confirm. Still log a ReplicationLogEntry per
 * follower once each background write completes.
 *
 * TRAINEE ACTION: implement only the synchronous replication branch. This
 * file is validated by `ReplicationManagerTest`. The synchronous tests
 * only pass when each follower has the value by the time replicate()
 * returns.
 */
public class ReplicationManager {

    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();
    private final List<ReplicationLogEntry> log = new CopyOnWriteArrayList<>();

    /**
     * Replicate a write to all followers in the given mode.
     *
     * @param key       the key being written
     * @param value     the value being written
     * @param followers the follower nodes that should receive a copy
     * @param mode      SYNCHRONOUS or ASYNCHRONOUS
     */
    public void replicate(String key, String value, List<ClusterNode> followers, ReplicationMode mode) {
        if (mode == ReplicationMode.SYNCHRONOUS) {
            // TODO (required): for each follower — record start time,
            // follower.put(key, value), record elapsed time, add a
            // ReplicationLogEntry to `log`. All of this must happen
            // before replicate() returns.
            throw new UnsupportedOperationException("TODO: implement synchronous replication");
        } else {
            // TODO (stretch goal, optional): submit a task to
            // asyncExecutor that does the same follower.put(...) +
            // logging, but replicate() itself returns immediately without
            // waiting for the task to finish. Fine to leave as-is if
            // you're short on time.
            throw new UnsupportedOperationException("TODO (stretch goal): implement asynchronous replication");
        }
    }

    public List<ReplicationLogEntry> getLog() {
        return List.copyOf(log);
    }

    public void shutdown() {
        asyncExecutor.shutdown();
    }
}
