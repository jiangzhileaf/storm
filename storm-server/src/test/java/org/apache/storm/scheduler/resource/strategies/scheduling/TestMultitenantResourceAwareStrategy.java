package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.networktopography.HostRackDNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.SupervisorResources;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.MultitenantResourceAwareScheduler;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesRule;
import org.apache.storm.scheduler.resource.strategies.priority.MultitenantSchedulingPriorityStrategy;
import org.apache.storm.topology.SharedOffHeapWithinNode;
import org.apache.storm.topology.SharedOffHeapWithinWorker;
import org.apache.storm.topology.SharedOnHeap;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.createClusterConfig;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genExecsAndComps;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisors;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userResourcePool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMultitenantResourceAwareStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TestMultitenantResourceAwareStrategy.class);

    private static final int CURRENT_TIME = 1450418597;

    @Rule
    public NormalizedResourcesRule nrRule = new NormalizedResourcesRule();

    /**
     * test if the scheduling logic for the MultitenantResourceAwareStrategy is correct
     */
    @Test
    public void testMultitenantResourceAwareStrategySharedMemory() {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        int numBolts = 3;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeap = 500;
        double sharedOffHeapNode = 700;
        double sharedOffHeapWorker = 500;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWorker, "bolt-1 shared off heap worker")).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapNode, "bolt-2 shared node")).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOnHeap(sharedOnHeap, "bolt-3 shared worker")).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 500, 2000, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0));
        Config conf = createClusterConfig(cpuPercent, memoryOnHeap, memoryOffHeap, null);

        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, MultitenantResourceAwareStrategy.class.getName());
        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 1,
                genExecsAndComps(stormToplogy), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, conf);

        MultitenantResourceAwareScheduler rs = new MultitenantResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        for (Map.Entry<String, SupervisorResources> entry: cluster.getSupervisorsResourcesMap().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorResources resources = entry.getValue();
            assertTrue(supervisorId, resources.getTotalCpu() >= resources.getUsedCpu());
            assertTrue(supervisorId, resources.getTotalMem() >= resources.getUsedMem());
        }

        // Everything should fit in a single slot
        int totalNumberOfTasks = (spoutParallelism + (boltParallelism * numBolts));
        double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
        double totalExpectedOnHeap = (totalNumberOfTasks * memoryOnHeap) + sharedOnHeap;
        double totalExpectedWorkerOffHeap = (totalNumberOfTasks * memoryOffHeap) + sharedOffHeapWorker;

        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());
        assertEquals(1, assignment.getSlots().size());
        WorkerSlot ws = assignment.getSlots().iterator().next();
        String nodeId = ws.getNodeId();
        assertEquals(1, assignment.getNodeIdToTotalSharedOffHeapMemory().size());
        assertEquals(sharedOffHeapNode, assignment.getNodeIdToTotalSharedOffHeapMemory().get(nodeId), 0.01);
        assertEquals(1, assignment.getScheduledResources().size());
        WorkerResources resources = assignment.getScheduledResources().get(ws);
        assertEquals(totalExpectedCPU, resources.get_cpu(), 0.01);
        assertEquals(totalExpectedOnHeap, resources.get_mem_on_heap(), 0.01);
        assertEquals(totalExpectedWorkerOffHeap, resources.get_mem_off_heap(), 0.01);
        assertEquals(sharedOnHeap, resources.get_shared_mem_on_heap(), 0.01);
        assertEquals(sharedOffHeapWorker, resources.get_shared_mem_off_heap(), 0.01);
    }

    /**
     * test if the scheduling logic for the MultitenantResourceAwareStrategy is correct
     */
    @Test
    public void TestMultitenantResourceAwareStrategyFilterNode() {
        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("spout");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Double> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0);

        Map<String, SupervisorDetails> supMap = genSupervisors(2, 1, 0, 100, 760, resourceMap);
        supMap.putAll(
                genSupervisors(2, 1, 10, 1000, 8000, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 20.0)));
        supMap.putAll(
                genSupervisors(2, 1, 20, 10, 8000, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 5000.0)));
        supMap.putAll(
                genSupervisors(2, 1, 30, 1000, 80, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 5000.0)));
        Config conf = createClusterConfig(50, 250, 250,  null);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");
        conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, MultitenantResourceAwareStrategy.class.getName());
        conf.setWorkerMaxBandwidthMbps(25);

        Map<ExecutorDetails, String> execsAndComps = genExecsAndComps(stormToplogy);
        LOG.debug("execsAndComps: {}", execsAndComps);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 2,
                execsAndComps, CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, conf);

        MultitenantResourceAwareScheduler rs = new MultitenantResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment = cluster.getAssignmentById("testTopology-id");

        Assert.assertEquals(assignment, null);
    }

    /**
     * test if the scheduling logic for the MultitenantResourceAwareStrategy is correct
     */
    @Test
    public void TestMultitenantResourceAwareStrategy() {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Double> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0);
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 150, 1500, resourceMap);

        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 200.0, 2000.0, 500));

        Config conf = createClusterConfig(50, 250, 250,  resourceUserPool);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");
        conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, MultitenantResourceAwareStrategy.class.getName());
        conf.setWorkerMaxBandwidthMbps(25);

        Map<ExecutorDetails, String> execsAndComps = genExecsAndComps(stormToplogy);
        LOG.debug("execsAndComps: {}", execsAndComps);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 3,
                execsAndComps, CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, conf);

        MultitenantResourceAwareScheduler rs = new MultitenantResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        HashSet<HashSet<ExecutorDetails>> expectedScheduling = new HashSet<>();
        expectedScheduling.add(new HashSet<>(Arrays.asList(new ExecutorDetails(0, 0)))); //Spout
        expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(2, 2), //bolt-1
                new ExecutorDetails(4, 4), //bolt-2
                new ExecutorDetails(6, 6)))); //bolt-3
        expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(1, 1), //bolt-1
                new ExecutorDetails(3, 3), //bolt-2
                new ExecutorDetails(5, 5)))); //bolt-3
        HashSet<HashSet<ExecutorDetails>> foundScheduling = new HashSet<>();
        SchedulerAssignment assignment = cluster.getAssignmentById("testTopology-id");
        if(assignment != null) {
            for (Collection<ExecutorDetails> execs : assignment.getSlotToExecutors().values()) {
                foundScheduling.add(new HashSet<>(execs));
            }
        }

        Assert.assertEquals(expectedScheduling, foundScheduling);
    }

    @Test
    public void TestMultitenantResourceAwareStrategyWithoutBandwidth() {
        final Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 0, 800, 8000, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0));

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8000);
        config.setNumWorkers(1);
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();

        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", config, 8, 0, 1, 0, CURRENT_TIME - 2, 10, "user");
        TopologyDetails topo2 = genTopology("topo-2", config, 8, 0, 1, 0, CURRENT_TIME - 2, 10, "user");

        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

        List<String> supHostnames = new LinkedList<>();
        for (SupervisorDetails sup : supMap.values()) {
            supHostnames.add(sup.getHost());
        }
        Map<String, List<String>> rackToNodes = new HashMap<>();

        DNSToSwitchMapping dns = new HostRackDNSToSwitchMapping();

        Map<String, String> resolvedSuperVisors =  dns.resolve(supHostnames);
        for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
            String hostName = entry.getKey();
            String rack = entry.getValue();
            List<String> nodesForRack = rackToNodes.get(rack);
            if (nodesForRack == null) {
                nodesForRack = new ArrayList<>();
                rackToNodes.put(rack, nodesForRack);
            }
            nodesForRack.add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        MultitenantResourceAwareStrategy rs = new MultitenantResourceAwareStrategy();

        rs.prepare(cluster);

        SchedulingResult schedulingResult = rs.schedule(cluster, topo1);
        assert(schedulingResult.isSuccess());
        SchedulerAssignment assignment = cluster.getAssignmentById(topo1.getId());
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());
    }

}
