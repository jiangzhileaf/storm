package org.apache.storm.scheduler.resource.strategies.priority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultitenantSchedulingPriorityStrategy implements ISchedulingPriorityStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(MultitenantSchedulingPriorityStrategy.class);

    protected MultitenantSchedulingPriorityStrategy.SimulatedUser getSimulatedUserFor(User u, ISchedulingState cluster) {
        return new MultitenantSchedulingPriorityStrategy.SimulatedUser(u, cluster);
    }

    @Override
    public List<TopologyDetails> getOrderedTopologies(ISchedulingState cluster, Map<String, User> userMap) {
        double cpuAvail = cluster.getClusterTotalCpuResource();
        double memAvail = cluster.getClusterTotalMemoryResource();
        double bandwidthAvail = cluster.getClusterTotalBandwidthResource();

        List<TopologyDetails> allUserTopologies = new ArrayList<>();
        List<MultitenantSchedulingPriorityStrategy.SimulatedUser> users = new ArrayList<>();
        for (User u : userMap.values()) {
            users.add(getSimulatedUserFor(u, cluster));
        }
        while (!users.isEmpty()) {
            Collections.sort(users, new MultitenantSchedulingPriorityStrategy.SimulatedUserComparator(cpuAvail, memAvail, bandwidthAvail));
            MultitenantSchedulingPriorityStrategy.SimulatedUser u = users.get(0);
            TopologyDetails td = u.getNextHighest();
            if (td == null) {
                users.remove(0);
            } else {
                double score = u.getScore(cpuAvail, memAvail, bandwidthAvail);
                td = u.simScheduleNextHighest();
                LOG.debug("SIM Scheduling {} with score of {}", td.getId(), score);
                cpuAvail -= td.getTotalRequestedCpu();
                memAvail -= (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap());
                allUserTopologies.add(td);
            }
        }
        return allUserTopologies;
    }

    protected static class SimulatedUser {
        public final double guaranteedCpu;
        public final double guaranteedMemory;
        public final double guaranteedBandwidth;
        protected final LinkedList<TopologyDetails> tds = new LinkedList<>();
        private double assignedCpu = 0.0;
        private double assignedMemory = 0.0;
        private double assignedBandwidth = 0.0;

        public SimulatedUser(User other, ISchedulingState cluster) {
            tds.addAll(cluster.getTopologies().getTopologiesOwnedBy(other.getId()));
            Collections.sort(tds, new MultitenantSchedulingPriorityStrategy.TopologyByPriorityAndSubmissionTimeComparator());
            Double guaranteedCpu = other.getCpuResourceGuaranteed();
            if (guaranteedCpu == null) {
                guaranteedCpu = 0.0;
            }
            this.guaranteedCpu = guaranteedCpu;
            Double guaranteedMemory = other.getMemoryResourceGuaranteed();
            if (guaranteedMemory == null) {
                guaranteedMemory = 0.0;
            }
            this.guaranteedMemory = guaranteedMemory;
            Double guaranteedBandwidth = other.getBandwidthResourceGuaranteed();
            if (guaranteedBandwidth == null) {
                guaranteedBandwidth = 0.0;
            }
            this.guaranteedBandwidth = guaranteedBandwidth;
        }

        public TopologyDetails getNextHighest() {
            return tds.peekFirst();
        }

        public TopologyDetails simScheduleNextHighest() {
            TopologyDetails td = tds.pop();
            assignedCpu += td.getTotalRequestedCpu();
            assignedMemory += td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap();
            assignedBandwidth += td.getTotalRequestedBandwidth();
            return td;
        }

        /**
         * Get a score for the simulated user.  This is used to sort the users, by their highest priority topology.
         * The only requirement is that if the user is over their guarantees, or there are no available resources the
         * returned score will be > 0.  If they are under their guarantee it must be negative.
         *
         * @param availableCpu    available CPU on the cluster.
         * @param availableMemory available memory on the cluster.
         * @param td              the topology we are looking at.
         * @return the score.
         */
        protected double getScore(double availableCpu, double availableMemory, double availableBandwidth, TopologyDetails td) {
            //(Requested + Assigned - Guaranteed)/Available
            if (td == null || availableCpu <= 0 || availableMemory <= 0 || availableBandwidth <= 0) {
                return Double.MAX_VALUE;
            }
            double wouldBeCpu = assignedCpu + td.getTotalRequestedCpu();
            double wouldBeMem = assignedMemory + td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap();
            double wouldBeBandwidth = assignedBandwidth + td.getTotalRequestedBandwidth();
            double cpuScore = (wouldBeCpu - guaranteedCpu) / availableCpu;
            double memScore = (wouldBeMem - guaranteedMemory) / availableMemory;
            double bandwidthScore = (wouldBeBandwidth - guaranteedBandwidth) / availableBandwidth;
            return Math.max(Math.max(cpuScore, memScore), bandwidthScore);
        }

        public double getScore(double availableCpu, double availableMemory, double availableBandwidth) {
            TopologyDetails td = getNextHighest();
            double score = getScore(availableCpu, availableMemory, availableBandwidth, td);
            if (td != null) {
                LOG.debug("id:{},score:{}", td.getId(), score);
            }
            return score;
        }

    }

    private static class SimulatedUserComparator implements Comparator<SimulatedUser> {

        private final double cpuAvail;
        private final double memAvail;
        private final double bandwidthAvail;

        private SimulatedUserComparator(double cpuAvail, double memAvail, double bandwidthAvail) {
            this.cpuAvail = cpuAvail;
            this.memAvail = memAvail;
            this.bandwidthAvail = bandwidthAvail;
        }

        @Override
        public int compare(MultitenantSchedulingPriorityStrategy.SimulatedUser o1, MultitenantSchedulingPriorityStrategy.SimulatedUser o2) {

            int ret = Double.compare(o1.getScore(cpuAvail, memAvail, bandwidthAvail), o2.getScore(cpuAvail, memAvail, bandwidthAvail));

            if (ret == 0) {
                Comparator cmp = new TopologyByPriorityAndSubmissionTimeComparator();
                ret = cmp.compare(o1.getNextHighest(), o2.getNextHighest());
            }

            return ret;
        }
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time.
     * First sort by Topology Priority, if there is a tie for topology priority, topology uptime is used to sort
     */
    private static class TopologyByPriorityAndSubmissionTimeComparator implements Comparator<TopologyDetails> {

        @Override
        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            if (topo2 == null) {
                return 1;
            }

            if (topo1 == null && topo2 != null) {
                return -1;
            }

            if (topo1.getTopologyPriority() > topo2.getTopologyPriority()) {
                return 1;
            } else if (topo1.getTopologyPriority() < topo2.getTopologyPriority()) {
                return -1;
            } else {
                if (topo1.getUpTime() > topo2.getUpTime()) {
                    return -1;
                } else if (topo1.getUpTime() < topo2.getUpTime()) {
                    return 1;
                } else {
                    return topo1.getId().compareTo(topo2.getId());
                }
            }
        }
    }
}
