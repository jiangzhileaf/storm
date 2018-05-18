/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.priority;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.INimbusTest;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesOrder;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.createClusterConfig;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisors;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.getUserMap;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userResourcePool;

public class TestMultitenantSchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestMultitenantSchedulingPriorityStrategy.class);

    @Test
    public void testSortUserLackOfResource() {

        LOG.info("\n\n\t\ttestSortUserLackOfResource");

        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {

            INimbus iNimbus = new INimbusTest();

            Map<String, Double> resourceMap = new HashMap<>();
            resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 1000.0);

            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0, resourceMap);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                    new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 200.0, 2000.0, 500));

            Config config = createClusterConfig(100, 500, 1000, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
            config.setWorkerMaxBandwidthMbps(1000);

            Topologies topologies = new Topologies(
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 0, "jerry"),
                    genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 1, "bobby"),
                    genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 2, "bobby"),
                    genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 3, "jerry"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            ISchedulingPriorityStrategy schedulingPriorityStrategy =
                    ReflectionUtils.newInstance((String) config.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));

            List<TopologyDetails> orderedTopologies = schedulingPriorityStrategy.getOrderedTopologies(cluster, getUserMap(resourceUserPool, cluster));

            assertTopologiesOrder(orderedTopologies, "topo-1-jerry", "topo-4-derek","topo-2-bobby","topo-3-bobby");

        }
    }

    @Test
    public void testSortWithoutBandwidthConfig1() {

        LOG.info("\n\n\t\ttestSortLackOfBandwidthConfig");

        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {

            INimbus iNimbus = new INimbusTest();

            Map<String, Double> resourceMap = new HashMap<>();
            resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0);

            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0, resourceMap);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                    new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 2000.0, 2000.0),
                    new TestUtilsForResourceAwareScheduler.TestUserResources("bobby", 2000.0, 2000.0));

            Config config = createClusterConfig(100, 500, 500, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());

            Topologies topologies = new Topologies(
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 0, "jerry"),
                    genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 1, "bobby"),
                    genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 2, "bobby"),
                    genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 3, "jerry"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            ISchedulingPriorityStrategy schedulingPriorityStrategy =
                    ReflectionUtils.newInstance((String) config.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));

            List<TopologyDetails> orderedTopologies = schedulingPriorityStrategy.getOrderedTopologies(cluster, getUserMap(resourceUserPool, cluster));

            assertTopologiesOrder(orderedTopologies, "topo-1-jerry","topo-2-bobby","topo-3-bobby","topo-4-derek");

        }
    }

    @Test
    public void testSortWithoutBandwidthConfig2() {

        LOG.info("\n\n\t\ttestSortLackOfBandwidthConfig");

        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {

            INimbus iNimbus = new INimbusTest();

            //if try to ignore a resource , please set huge value in user-resource-pools.yaml
            Map<String, Double> resourceMap = new HashMap<>();
            resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 999999999999.0);

            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0, resourceMap);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                    new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 80.0, 2000.0),
                    new TestUtilsForResourceAwareScheduler.TestUserResources("bobby", 2000.0, 2000.0));

            Config config = createClusterConfig(100, 500, 500, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());

            Topologies topologies = new Topologies(
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 0, "jerry"),
                    genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 1, "bobby"),
                    genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 2, "bobby"),
                    genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 3, "jerry"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            ISchedulingPriorityStrategy schedulingPriorityStrategy =
                    ReflectionUtils.newInstance((String) config.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));

            List<TopologyDetails> orderedTopologies = schedulingPriorityStrategy.getOrderedTopologies(cluster, getUserMap(resourceUserPool, cluster));

            assertTopologiesOrder(orderedTopologies, "topo-2-bobby","topo-3-bobby","topo-1-jerry","topo-4-derek");

        }
    }

    @Test
    public void testSortWithEnoughSource() {

        LOG.info("\n\n\t\ttestSortWithEnoughSource");

        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {

            INimbus iNimbus = new INimbusTest();

            Map<String, Double> resourceMap = new HashMap<>();
            resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 100.0);

            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0, resourceMap);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                    new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 2000.0, 2000.0, 2000),
                    new TestUtilsForResourceAwareScheduler.TestUserResources("bobby", 2000.0, 2000.0, 2000));

            Config config = createClusterConfig(100, 500, 500, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
            config.setWorkerMaxBandwidthMbps(25);

            Topologies topologies = new Topologies(
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 0, "jerry"),
                    genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 1, "bobby"),
                    genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 2, "bobby"),
                    genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 2, "jerry"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            ISchedulingPriorityStrategy schedulingPriorityStrategy =
                    ReflectionUtils.newInstance((String) config.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));

            List<TopologyDetails> orderedTopologies = schedulingPriorityStrategy.getOrderedTopologies(cluster, getUserMap(resourceUserPool, cluster));

            assertTopologiesOrder(orderedTopologies, "topo-1-jerry","topo-2-bobby","topo-3-bobby","topo-4-derek");

        }
    }

    @Test
    public void testSortUserLackOfResourceAfterFirstAssigned() {

        LOG.info("\n\n\t\ttestSortUserLackOfResourceAfterFirstAssigned");

        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {

            INimbus iNimbus = new INimbusTest();

            Map<String, Double> resourceMap = new HashMap<>();
            resourceMap.put(Config.SUPERVISOR_BANDWIDTH_CAPACITY_MBPS, 1000.0);

            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0, resourceMap);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                    new TestUtilsForResourceAwareScheduler.TestUserResources("jerry", 2000.0, 2000.0, 2000),
                    new TestUtilsForResourceAwareScheduler.TestUserResources("bobby", 100.0, 1000.0, 1000));

            Config config = createClusterConfig(100, 500, 500, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, MultitenantSchedulingPriorityStrategy.class.getName());
            config.setWorkerMaxBandwidthMbps(1000);

            Topologies topologies = new Topologies(
                    genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 0, "jerry"),
                    genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 1, "bobby"),
                    genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 2, "bobby"),
                    genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 3, "jerry"));
            Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);

            ISchedulingPriorityStrategy schedulingPriorityStrategy =
                    ReflectionUtils.newInstance((String) config.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));

            List<TopologyDetails> orderedTopologies = schedulingPriorityStrategy.getOrderedTopologies(cluster, getUserMap(resourceUserPool, cluster));

            assertTopologiesOrder(orderedTopologies, "topo-1-jerry","topo-2-bobby", "topo-4-derek","topo-3-bobby");

        }
    }
}
