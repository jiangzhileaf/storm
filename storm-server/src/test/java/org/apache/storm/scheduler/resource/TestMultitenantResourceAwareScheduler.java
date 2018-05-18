package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.strategies.priority.FIFOSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.MultitenantSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.MultitenantResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.TestMultitenantResourceAwareStrategy;
import org.apache.storm.utils.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.addTopologies;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesNotScheduled;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.createClusterConfig;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisors;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userResourcePool;

public class TestMultitenantResourceAwareScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestMultitenantResourceAwareScheduler.class);

    @Test
    public void testEvictTopolgy(){
        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
            INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();

            Map<String, SupervisorDetails> supMap = genSupervisors(1, 1, 100.0, 1000.0, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 1000.0));

            Config config = createClusterConfig(100, 500, 500, null);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
            config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, MultitenantResourceAwareStrategy.class.getName());

            Topologies topologies = new Topologies(
                    genTopology("topo-4-derek", config, 1, 0, 1, 0,Time.currentTimeSecs() - 201,29, "derek"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            MultitenantResourceAwareScheduler rs = new MultitenantResourceAwareScheduler();
            rs.prepare(config);
            rs.schedule(topologies, cluster);

            assertTopologiesFullyScheduled(cluster,  "topo-4-derek");

            LOG.info("\n\n\t\tINSERTING topo-5");
            //new topology needs to be scheduled
            //topo-3 should be evicted since its been up the longest
            topologies = addTopologies(topologies,
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0,Time.currentTimeSecs() - 250,20, "jerry"));

            cluster = new Cluster(cluster.getINimbus(), cluster.getSupervisors(), cluster.getAssignments(), topologies, config);

            rs.schedule(topologies, cluster);

            assertTopologiesFullyScheduled(cluster, "topo-1-jerry");
            assertTopologiesNotScheduled(cluster, "topo-4-derek");
        }
    }

    @Test
    public void testValidTopologyMaxHeapSize(){
        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
            INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();

            Map<String, SupervisorDetails> supMap = genSupervisors(1, 1, 100.0, 1000.0, Collections.singletonMap(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 1000.0));

            Config config = createClusterConfig(100, 500, 500, null);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
            config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, MultitenantResourceAwareStrategy.class.getName());
            config.setNumWorkers(1);
            config.setTopologyWorkerMaxHeapSize(250);

            Topologies topologies = new Topologies(
                    genTopology("topo-4-derek", config, 1, 0, 1, 0,Time.currentTimeSecs() - 201,29, "derek"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            MultitenantResourceAwareScheduler rs = new MultitenantResourceAwareScheduler();
            rs.prepare(config);
            rs.schedule(topologies, cluster);

            assertTopologiesNotScheduled(cluster, "topo-4-derek");
        }
    }

}
