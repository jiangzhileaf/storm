package org.apache.storm.scheduler.resource;

import com.google.common.collect.ImmutableList;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SingleTopologyCluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.scheduler.utils.ConfigLoaderFactoryService;
import org.apache.storm.scheduler.utils.IConfigLoader;
import org.apache.storm.utils.DisallowedStrategyException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultitenantResourceAwareScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceAwareScheduler.class);
    private Map<String, Object> conf;
    private ISchedulingPriorityStrategy schedulingPriorityStrategy;
    private IConfigLoader configLoader;
    private int maxSchedulingAttempts;

    private static void markFailedTopology(User u, Cluster c, TopologyDetails td, String message) {
        markFailedTopology(u, c, td, message, null);
    }

    private static void markFailedTopology(User u, Cluster c, TopologyDetails td, String message, Throwable t) {
        c.setStatus(td, message);
        String realMessage = td.getId() + " " + message;
        if (t != null) {
            LOG.error(realMessage, t);
        } else {
            LOG.error(realMessage);
        }
        u.markTopoUnsuccess(td);
    }

    private static double getCpuUsed(SchedulerAssignment assignment) {
        return assignment.getScheduledResources().values().stream().mapToDouble((wr) -> wr.get_cpu()).sum();
    }

    private static double getMemoryUsed(SchedulerAssignment assignment) {
        return assignment.getScheduledResources().values().stream()
                .mapToDouble((wr) -> wr.get_mem_on_heap() + wr.get_mem_off_heap()).sum();
    }

    private static double getBandwidthUsed(SchedulerAssignment assignment, TopologyDetails td) {
        int workerNum = assignment.getSlots().size();
        return workerNum * ObjectReader.getDouble(td.getConf().get(Config.TOPOLOGY_WORKER_MAX_BANDWIDTH_MBPS)).intValue();
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        this.conf = conf;
        schedulingPriorityStrategy = ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));
        configLoader = ConfigLoaderFactoryService.createConfigLoader(conf);
        maxSchedulingAttempts = ObjectReader.getInt(
                conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_TOPOLOGY_SCHEDULING_ATTEMPTS), 5);
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return (Map) getUserResourcePools();
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Map<String, User> userMap = getUsers(cluster);
        List<TopologyDetails> orderedTopologies = new ArrayList<>(schedulingPriorityStrategy.getOrderedTopologies(cluster, userMap));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ordered list of topologies is: {}", orderedTopologies.stream().map((t) -> t.getId()).collect(Collectors.toList()));
        }
        for (TopologyDetails td : orderedTopologies) {
            if (!cluster.needsSchedulingRas(td)) {
                //cluster forgets about its previous status, so if it is scheduled just leave it.
                cluster.setStatusIfAbsent(td.getId(), "Fully Scheduled");
            } else {
                User submitter = userMap.get(td.getTopologySubmitter());
                scheduleTopology(td, cluster, submitter, orderedTopologies);
            }
        }
    }

    private boolean validWorkerMaxHeapSize(TopologyDetails td){
        return (td.getTotalRequestedMemOnHeap()) / td.getNumWorkers() < td.getTopologyWorkerMaxHeapSize();
    }

    private void scheduleTopology(TopologyDetails td, Cluster cluster, final User topologySubmitter,
                                  List<TopologyDetails> orderedTopologies) {
        //check the worker number and the max heap size
        if(!validWorkerMaxHeapSize(td)){
            markFailedTopology(topologySubmitter, cluster, td,
                    "Unsuccessful in scheduling - worker max heap size is too small!");
            return;
        }

        //A copy of cluster that we can modify, but does not get committed back to cluster unless scheduling succeeds
        Cluster workingState = new Cluster(cluster);
        RAS_Nodes nodes = new RAS_Nodes(workingState);
        IStrategy rasStrategy = null;
        String strategyConf = (String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY);
        try {
            String strategy = (String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY);
            if (strategy.startsWith("backtype.storm")) {
                // Storm supports to launch workers of older version.
                // If the config of TOPOLOGY_SCHEDULER_STRATEGY comes from the older version, replace the package name.
                strategy = strategy.replace("backtype.storm", "org.apache.storm");
                LOG.debug("Replace backtype.storm with org.apache.storm for Config.TOPOLOGY_SCHEDULER_STRATEGY");
            }
            rasStrategy = ReflectionUtils.newSchedulerStrategyInstance(strategy, conf);
            rasStrategy.prepare(conf);
        } catch (DisallowedStrategyException e) {
            markFailedTopology(topologySubmitter, cluster, td,
                    "Unsuccessful in scheduling - " + e.getAttemptedClass()
                            + " is not an allowed strategy. Please make sure your "
                            + Config.TOPOLOGY_SCHEDULER_STRATEGY
                            + " config is one of the allowed strategies: "
                            + e.getAllowedStrategies(), e);
            return;
        } catch (RuntimeException e) {
            markFailedTopology(topologySubmitter, cluster, td,
                    "Unsuccessful in scheduling - failed to create instance of topology strategy "
                            + strategyConf
                            + ". Please check logs for details", e);
            return;
        }

        for (int i = 0; i < maxSchedulingAttempts; i++) {
            SingleTopologyCluster toSchedule = new SingleTopologyCluster(workingState, td.getId());
            try {
                SchedulingResult result = rasStrategy.schedule(toSchedule, td);
                LOG.debug("scheduling result: {}", result);
                if (result == null) {
                    markFailedTopology(topologySubmitter, cluster, td, "Internal scheduler error");
                    return;
                } else {
                    if (result.isSuccess()) {
                        cluster.updateFrom(toSchedule);
                        cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                        //DONE
                        return;
                    } else if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                        LOG.debug("Not enough resources to schedule {}", td.getName());
                        List<TopologyDetails> reversedList = ImmutableList.copyOf(orderedTopologies).reverse();
                        boolean evictedSomething = false;
                        LOG.debug("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
                        int tdIndex = reversedList.indexOf(td);
                        double cpuNeeded = td.getTotalRequestedCpu();
                        double memoryNeeded = td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap();
                        double bandwidthNeeded = td.getTotalRequestedBandwidth();
                        SchedulerAssignment assignment = cluster.getAssignmentById(td.getId());
                        if (assignment != null) {
                            cpuNeeded -= getCpuUsed(assignment);
                            memoryNeeded -= getMemoryUsed(assignment);
                            bandwidthNeeded -= getBandwidthUsed(assignment, td);
                        }
                        for (int index = 0; index < tdIndex; index++) {
                            TopologyDetails topologyEvict = reversedList.get(index);
                            SchedulerAssignment evictAssignemnt = workingState.getAssignmentById(topologyEvict.getId());
                            if (evictAssignemnt != null && !evictAssignemnt.getSlots().isEmpty()) {
                                Collection<WorkerSlot> workersToEvict = workingState.getUsedSlotsByTopologyId(topologyEvict.getId());

                                LOG.debug("Evicting Topology {} with workers: {} from user {}", topologyEvict.getName(), workersToEvict,
                                        topologyEvict.getTopologySubmitter());
                                cpuNeeded -= getCpuUsed(evictAssignemnt);
                                memoryNeeded -= getMemoryUsed(evictAssignemnt);
                                bandwidthNeeded -= getBandwidthUsed(evictAssignemnt, topologyEvict);
                                nodes.freeSlots(workersToEvict);
                                if (cpuNeeded <= 0 && memoryNeeded <= 0 && bandwidthNeeded <=0) {
                                    //We evicted enough topologies to have a hope of scheduling, so try it now, and don't evict more
                                    // than is needed
                                    evictedSomething = true;
                                    break;
                                }
                            }
                        }

                        if (!evictedSomething) {
                            StringBuilder message = new StringBuilder();
                            message.append("Not enough resources to schedule ");
                            if (memoryNeeded > 0 || cpuNeeded > 0 || bandwidthNeeded > 0) {
                                if (memoryNeeded > 0) {
                                    message.append(memoryNeeded).append(" MB ");
                                }
                                if (cpuNeeded > 0) {
                                    message.append(cpuNeeded).append("% CPU ");
                                }
                                if (bandwidthNeeded > 0) {
                                    message.append(bandwidthNeeded).append(" Mbps ");
                                }
                                message.append("needed even after evicting lower priority topologies. ");
                            }
                            message.append(result.getErrorMessage());
                            markFailedTopology(topologySubmitter, cluster, td, message.toString());
                            return;
                        }
                        //Only place we fall though to do the loop over again...
                    } else { //Any other failure result
                        //The assumption is that the strategy set the status...
                        topologySubmitter.markTopoUnsuccess(td, cluster);
                        return;
                    }
                }
            } catch (Exception ex) {
                markFailedTopology(topologySubmitter, cluster, td,
                        "Internal Error - Exception thrown when scheduling. Please check logs for details", ex);
                return;
            }
        }
        markFailedTopology(topologySubmitter, cluster, td, "Failed to schedule within " + maxSchedulingAttempts + " attempts");
    }

    /**
     * Get User wrappers around cluster.
     *
     * @param cluster the cluster to get the users out of.
     */
    private Map<String, User> getUsers(Cluster cluster) {
        Map<String, User> userMap = new HashMap<>();
        Map<String, Map<String, Double>> userResourcePools = getUserResourcePools();
        LOG.debug("userResourcePools: {}", userResourcePools);

        for (TopologyDetails td : cluster.getTopologies()) {
            String topologySubmitter = td.getTopologySubmitter();
            //additional safety check to make sure that topologySubmitter is going to be a valid value
            if (topologySubmitter == null || topologySubmitter.equals("")) {
                LOG.error("Cannot determine user for topology {}.  Will skip scheduling this topology", td.getName());
                continue;
            }
            if (!userMap.containsKey(topologySubmitter)) {
                userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
        }
        return userMap;
    }

    private Map<String, Map<String, Double>> convertToDouble(Map<String, Map<String, Number>> raw) {
        Map<String, Map<String, Double>> ret = new HashMap<>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : raw.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        return ret;
    }

    /**
     * Get resource guarantee configs.
     * Load from configLoaders first; if no config available, read from user-resource-pools.yaml;
     * if no config available from user-resource-pools.yaml, get configs from conf. Only one will be used.
     * @return a map that contains resource guarantees of every user of the following format
     *     {userid->{resourceType->amountGuaranteed}}
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {
        Map<String, Map<String, Number>> raw;

        // Try the loader plugin, if configured
        if (configLoader != null) {
            raw = (Map<String, Map<String, Number>>) configLoader.load(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
            if (raw != null) {
                return convertToDouble(raw);
            } else {
                LOG.warn("Config loader returned null. Will try to read from user-resource-pools.yaml");
            }
        }

        // if no configs from user-resource-pools.yaml, get configs from conf
        raw = (Map<String, Map<String, Number>>) conf.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (raw != null) {
            return convertToDouble(raw);
        } else {
            LOG.warn("Reading from storm configuration returned null. Will load configs from user-resource-pools.yaml.");
        }

        // if no configs from loader, try to read from user-resource-pools.yaml
        Map<String, Object> fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        raw = (Map<String, Map<String, Number>>) fromFile.get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_USER_POOLS);


        return convertToDouble(raw);
    }
}
