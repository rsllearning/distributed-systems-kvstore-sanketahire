package com.hld.kvstore.cluster;

import com.hld.kvstore.consistency.APConsistencyStrategy;
import com.hld.kvstore.consistency.CPConsistencyStrategy;
import com.hld.kvstore.node.ClusterNode;
import com.hld.kvstore.node.NodeUnreachableException;
import com.hld.kvstore.partition.PartitionSimulator;
import com.hld.kvstore.replication.ReplicationManager;
import com.hld.kvstore.replication.ReplicationMode;
import com.hld.kvstore.ring.ConsistentHashingStrategy;
import com.hld.kvstore.ring.HashingStrategy;
import com.hld.kvstore.ring.NaiveHashingStrategy;

import java.util.*;

/**
 * Wire everything together and run the live system demonstrations.
 *
 * Implements the two required demos:
 *   1. demoRingDistribution():
 *        - Measures key distribution across 4 nodes.
 *        - Evaluates rebalancing churn when a 5th node joins.
 *        - Compares Consistent Hashing with Virtual Nodes against Naive Hashing.
 *   2. demoCapBehavior():
 *        - Demonstrates CP mode: read is rejected with NodeUnreachableException
 *          when leader is partitioned (Consistency prioritized over Availability).
 *        - Demonstrates AP mode: read gracefully falls back to secondary replica
 *          when leader is partitioned (Availability prioritized over Consistency).
 */
public class Main {

    public static void main(String[] args) {
        System.out.println("======================================================");
        System.out.println("       DISTRIBUTED KEY-VALUE STORE DEMONSTRATION      ");
        System.out.println("======================================================\n");

        demoRingDistribution();
        System.out.println();
        demoCapBehavior();

        System.out.println("\n======================================================");
        System.out.println("            ALL DEMONSTRATIONS COMPLETED              ");
        System.out.println("======================================================");
    }

    /**
     * Demo 1: Demonstrates consistent hashing rebalance efficiency vs. naive modulo hashing.
     */
    private static void demoRingDistribution() {
        System.out.println("------------------------------------------------------");
        System.out.println(" DEMO 1: Key Distribution & Rebalancing Churn Comparison");
        System.out.println("------------------------------------------------------");

        int totalKeys = 1_000;
        List<String> testKeys = new ArrayList<>(totalKeys);
        for (int i = 0; i < totalKeys; i++) {
            testKeys.add("key-" + UUID.randomUUID());
        }

        // 1. Consistent Hashing Strategy
        ConsistentHashingStrategy consistent = new ConsistentHashingStrategy();
        for (int i = 1; i <= 4; i++) {
            consistent.addNode("node-" + i);
        }

        Map<String, String> consistentBefore = new HashMap<>();
        Map<String, Integer> consistentCount = new TreeMap<>();
        for (String k : testKeys) {
            String owner = consistent.getOwnerNode(k);
            consistentBefore.put(k, owner);
            consistentCount.merge(owner, 1, Integer::sum);
        }

        System.out.println("\n[Consistent Hashing with 100 Vnodes] 4 Nodes Key Distribution:");
        consistentCount.forEach((node, count) ->
                System.out.printf("  %s: %d keys (%.1f%%)%n", node, count, (count * 100.0) / totalKeys));

        consistent.addNode("node-5");
        int consistentMoved = 0;
        for (String k : testKeys) {
            if (!consistentBefore.get(k).equals(consistent.getOwnerNode(k))) {
                consistentMoved++;
            }
        }
        double consistentMovedPercent = (consistentMoved * 100.0) / totalKeys;

        // 2. Naive Hashing Strategy
        NaiveHashingStrategy naive = new NaiveHashingStrategy();
        for (int i = 1; i <= 4; i++) {
            naive.addNode("node-" + i);
        }

        Map<String, String> naiveBefore = new HashMap<>();
        Map<String, Integer> naiveCount = new TreeMap<>();
        for (String k : testKeys) {
            String owner = naive.getOwnerNode(k);
            naiveBefore.put(k, owner);
            naiveCount.merge(owner, 1, Integer::sum);
        }

        naive.addNode("node-5");
        int naiveMoved = 0;
        for (String k : testKeys) {
            if (!naiveBefore.get(k).equals(naive.getOwnerNode(k))) {
                naiveMoved++;
            }
        }
        double naiveMovedPercent = (naiveMoved * 100.0) / totalKeys;

        System.out.println("\n[Rebalancing Comparison upon adding 5th node]:");
        System.out.printf("  • Naive Hashing Keys Moved:       %d / %d (%.2f%%)%n",
                naiveMoved, totalKeys, naiveMovedPercent);
        System.out.printf("  • Consistent Hashing Keys Moved:  %d / %d (%.2f%%)%n",
                consistentMoved, totalKeys, consistentMovedPercent);
        System.out.printf("  ==> Key Migration Reduction:      %.2f%% less churn!%n",
                naiveMovedPercent - consistentMovedPercent);
    }

    /**
     * Demo 2: Demonstrates CP vs AP behavior under network partitions.
     */
    private static void demoCapBehavior() {
        System.out.println("------------------------------------------------------");
        System.out.println(" DEMO 2: CAP Theorem In Action (CP vs AP Mode Under Partition)");
        System.out.println("------------------------------------------------------");

        PartitionSimulator simulator = new PartitionSimulator();
        String testKey = "user:profile:1001";
        String testValue = "{ name: 'Alice Smith', tier: 'premium' }";

        // A. CP Mode Demonstration
        System.out.println("\n[Part A: CP Mode — Strict Consistency, Uptime Sacrificed on Partition]");
        ConsistentHashingStrategy cpRing = new ConsistentHashingStrategy();
        ReplicationManager cpRepManager = new ReplicationManager();
        Cluster cpCluster = new Cluster(
                cpRing,
                cpRepManager,
                new CPConsistencyStrategy(),
                ReplicationMode.SYNCHRONOUS,
                2
        );

        for (int i = 1; i <= 3; i++) {
            cpCluster.addNode(new ClusterNode("node-" + i));
        }

        cpCluster.put(testKey, testValue);
        System.out.printf("  1. Written key '%s' to CP cluster.%n", testKey);

        String leaderId = cpRing.getOwnerNode(testKey);
        ClusterNode leaderNode = cpCluster.getNode(leaderId);

        System.out.printf("  2. Simulating network partition isolating leader [%s]...%n", leaderId);
        simulator.isolateNode(leaderNode);

        try {
            System.out.print("  3. Attempting to read key during partition... ");
            cpCluster.get(testKey);
            System.out.println("UNEXPECTED: Read succeeded!");
        } catch (NodeUnreachableException e) {
            System.out.println("REJECTED with NodeUnreachableException!");
            System.out.printf("     Reason: Leader [%s] unreachable. CP strictly refuses stale reads.%n", leaderId);
        }

        simulator.healNode(leaderNode);
        System.out.printf("  4. Partition healed. Leader [%s] restored to HEALTHY.%n", leaderId);
        cpRepManager.shutdown();

        // B. AP Mode Demonstration
        System.out.println("\n[Part B: AP Mode — High Availability, Read Fallback to Replica]");
        ConsistentHashingStrategy apRing = new ConsistentHashingStrategy();
        ReplicationManager apRepManager = new ReplicationManager();
        Cluster apCluster = new Cluster(
                apRing,
                apRepManager,
                new APConsistencyStrategy(),
                ReplicationMode.SYNCHRONOUS,
                2
        );

        for (int i = 1; i <= 3; i++) {
            apCluster.addNode(new ClusterNode("node-" + i));
        }

        apCluster.put(testKey, testValue);
        System.out.printf("  1. Written key '%s' to AP cluster (synchronously replicated to followers).%n", testKey);

        String apLeaderId = apRing.getOwnerNode(testKey);
        ClusterNode apLeaderNode = apCluster.getNode(apLeaderId);

        System.out.printf("  2. Simulating network partition isolating leader [%s]...%n", apLeaderId);
        simulator.isolateNode(apLeaderNode);

        System.out.print("  3. Attempting to read key during partition... ");
        Optional<String> apValue = apCluster.get(testKey);
        if (apValue.isPresent()) {
            System.out.println("SUCCESS! Read served gracefully via Fallback Replica.");
            System.out.printf("     Retrieved Value: %s%n", apValue.get());
        }

        System.out.println("  4. Attempting write to partitioned node in AP mode... ");
        new APConsistencyStrategy().handleWrite("user:profile:1002", "new-data", apLeaderNode);
        System.out.println("     Write degraded gracefully without throwing an exception.");

        simulator.healNode(apLeaderNode);
        System.out.printf("  5. Partition healed. Leader [%s] restored to HEALTHY.%n", apLeaderId);
        apRepManager.shutdown();
    }
}
