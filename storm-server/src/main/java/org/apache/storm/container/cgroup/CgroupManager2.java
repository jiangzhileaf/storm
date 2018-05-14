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

package org.apache.storm.container.cgroup;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.core.NetClsCore;
import org.apache.storm.container.tc.TcClass;
import org.apache.storm.container.tc.TcManager;
import org.apache.storm.container.tc.TcQdisc;
import org.apache.storm.container.tc.TcUtils;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that implements ResourceIsolationInterface that manages cgroups.
 */
public class CgroupManager2 implements ResourceIsolationInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CgroupManager2.class);

    private CgroupCenter center;

    private List<SubSystemType> subSystems;

    private Map<SubSystemType, CgroupCommon> rootCgroups;

    private String rootDir;

    private Map<String, Object> conf;

    private TcManager tcManager;

    /**
     * initialize data structures.
     *
     * @param conf storm confs
     */
    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;
        this.rootDir = DaemonConfig.getCgroupRootDir(this.conf);
        if (this.rootDir == null) {
            throw new RuntimeException("Check configuration file. The storm.supervisor.cgroup.rootdir is missing.");
        }

        this.subSystems = new ArrayList<>();
        for (String resource : DaemonConfig.getCgroupStormResources(conf)) {
            SubSystemType subSystem = SubSystemType.getSubSystem(resource);
            validCgroupStormHierarchyDir(subSystem);
            this.subSystems.add(subSystem);
        }

        this.center = CgroupCenter.getInstance();
        if (this.center == null) {
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        }
        this.prepareSubSystem(this.conf);

        this.tcManager = TcManager.getInstance();
    }

    private void validCgroupStormHierarchyDir(SubSystemType subSystem) {
        String cgroupRootDir = DaemonConfig.getCgroupStormHierarchyDir(conf);
        String hierarchyPath = cgroupRootDir + File.pathSeparator + subSystem.name() + File.pathSeparator + rootDir;
        File file = new File(hierarchyPath);
        if (!file.exists()) {
            LOG.error("{} does not exist", file.getPath());
            throw new RuntimeException(
                    "Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
    }

    /**
     * Initialize subsystems.
     */
    private void prepareSubSystem(Map<String, Object> conf) throws IOException {

        this.rootCgroups = new HashMap<>();

        for (SubSystemType subSystem : subSystems) {
            validCgroupStormHierarchyDir(subSystem);
            Hierarchy hierarchy = center.getHierarchyWithSubSystem(subSystem);
            CgroupCommon rootCgroup = new CgroupCommon(this.rootDir, hierarchy, hierarchy.getRootCgroups());
            this.rootCgroups.put(subSystem, rootCgroup);
        }

        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS
        // operations.
        CpuCore supervisorRootCpu = (CpuCore) this.rootCgroups.get(SubSystemType.cpu).getCores().get(SubSystemType.cpu);
        setCpuUsageUpperLimit(supervisorRootCpu, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }

    private CgroupCommon getWorkerGroup(String workerId, SubSystemType subSystem) {
        CgroupCommon rootCgroup = this.rootCgroups.get(subSystem);
        CgroupCommon workerGroup = new CgroupCommon(workerId, rootCgroup.getHierarchy(), rootCgroup);
        return workerGroup;
    }

    /**
     * Use cfs_period & cfs_quota to control the upper limit use of cpu core e.g.
     * If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {
        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer totalMem, Integer cpuNum) throws SecurityException {
        reserveResourcesForWorker(workerId, totalMem, cpuNum, null);
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer workerMemory, Integer workerCpu, Integer workerBandwidth) {
        LOG.info("Creating cgroup for worker {} with resources {} MB {} % CPU {} BandWidth", workerId, workerMemory, workerCpu, workerBandwidth);

        if(subSystems.contains(SubSystemType.cpu)){
            reserveCPUForWorker(workerId, workerCpu);
        }

        if(subSystems.contains(SubSystemType.memory)){
            reserveMemoryForWorker(workerId, workerMemory);
        }

        if(subSystems.contains(SubSystemType.net_cls)) {
            reserveBandWidthForWorker(workerId, workerBandwidth);
        }
    }

    private Set<String> findUsingTcClassId() {
        Set<String> usingTcClassId = new HashSet<>();
        CgroupCommon netClsGroup = this.rootCgroups.get(SubSystemType.net_cls);
        for (CgroupCommon workerGroup : netClsGroup.getChildren()) {
            String classIdPath = String.format("%s/net_cls.classid", workerGroup.getDir());
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(classIdPath));
                long decimalId = Long.parseLong(new String(bytes));
                String strId = TcClass.getIdInStr(decimalId);
                if (usingTcClassId.contains(strId)) {
                    throw new RuntimeException("class should not be shared!");
                } else {
                    usingTcClassId.add(strId);
                }
            } catch (IOException e) {
                throw new RuntimeException("read tc net_cls.classid error!");
            }

        }

        return usingTcClassId;
    }

    private Set<TcClass> getAvailableTcClass() {

        Set<TcClass> availableTcClass = new HashSet<>();

        try {
            Set<String> usingTcClassId = findUsingTcClassId();
            Set<String> validNICs = new HashSet<>((List<String>) conf.get(Config.SUPERVISOR_CONTROL_NICS));
            Set<String> excludeTcClassIds = new HashSet<>((List<String>) conf.get(Config.SUPERVISOR_TC_CLASS_EXCLUDE));
            List<TcQdisc> qdiscs = tcManager.getTCInfo();

            for (TcQdisc qdisc : qdiscs) {
                if (validNICs.contains(qdisc.getNetworkCard())) {
                    for (TcClass tclass : qdisc.getClasses()) {
                        String classId = tclass.getId();
                        if (!usingTcClassId.contains(classId) && !excludeTcClassIds.contains(classId)) {
                            availableTcClass.add(tclass);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("read tc qdisc info error!");
        }
        return availableTcClass;
    }

    private TcClass findTcClassByRate(Integer bandwidth) {
        Set<TcClass> availClass = getAvailableTcClass();
        for(TcClass tcClass : availClass){
            String rateStr = tcClass.getProps().get("rate");
            Integer rateMbps = TcUtils.parseRateStr(rateStr);
            if(rateMbps == bandwidth){
                return tcClass;
            }
        }
        throw new RuntimeException("could not find suitable tc class with match bandwidth!");
    }

    private void reserveBandWidthForWorker(String workerId, Integer bandwidth) {
        TcClass tcClass = findTcClassByRate(bandwidth);

        CgroupCommon workerGroup = getWorkerGroup(workerId, SubSystemType.net_cls);
        createWorkGroup(workerGroup);

        if(tcClass != null){
            NetClsCore netClsCore = (NetClsCore) workerGroup.getCores().get(SubSystemType.net_cls);
            try {
                netClsCore.setClassId(TcClass.getIdIndecimal(tcClass.getId()));
            } catch (IOException e) {
                throw new RuntimeException("Cannot set net_cls.classid! Exception: ", e);
            }
        }
    }

    private void reserveCPUForWorker(String workerId, Integer cpuNum) {

        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by
        // RAS (Resource Aware Scheduler)
        if (conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            cpuNum = ((Number) conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT)).intValue();
        }

        CgroupCommon workerGroup = getWorkerGroup(workerId, SubSystemType.cpu);
        createWorkGroup(workerGroup);

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuNum.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: ", e);
            }
        }
    }

    private void reserveMemoryForWorker(String workerId, Integer totalMem) {

        // The manually set STORM_WORKER_CGROUP_MEMORY_MB_LIMIT config on supervisor will overwrite
        // resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT) != null) {
            totalMem =
                    ((Number) this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT)).intValue();
        }

        CgroupCommon workerGroup = getWorkerGroup(workerId, SubSystemType.memory);
        createWorkGroup(workerGroup);

        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE)) {
            if (totalMem != null) {
                int cgroupMem =
                        (int)
                                (Math.ceil(
                                        ObjectReader.getDouble(
                                                this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB),
                                                0.0)));
                long memLimit = Long.valueOf((totalMem.longValue() + cgroupMem) * 1024 * 1024);
                MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
                try {
                    memCore.setPhysicalUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: ", e);
                }
                // need to set memory.memsw.limit_in_bytes after setting memory.limit_in_bytes or error
                // might occur
                try {
                    memCore.setWithSwapUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.memsw.limit_in_bytes! Exception: ", e);
                }
            }
        }
    }

    private void createWorkGroup(CgroupCommon workerGroup) {
        try {
            this.center.createCgroup(workerGroup);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating Cgroup! Exception: ", e);
        }
    }

    @Override
    public void releaseResourcesForWorker(String workerId) {
        for (CgroupCommon rootCgroup : rootCgroups.values()) {
            CgroupCommon workerGroup = new CgroupCommon(workerId, rootCgroup.getHierarchy(), rootCgroup);
            try {
                Set<Integer> tasks = workerGroup.getTasks();
                if (!tasks.isEmpty()) {
                    throw new Exception("Cannot correctly shutdown worker CGroup " + workerId + "tasks " + tasks
                            + " still running!");
                }
                this.center.deleteCgroup(workerGroup);
            } catch (Exception e) {
                LOG.error("Exception thrown when shutting worker {} Exception: {}", workerId, e);
            }
        }
    }

    @Override
    public List<String> getLaunchCommand(String workerId, List<String> existingCommand) {
        List<String> newCommand = getLaunchCommandPrefix(workerId);
        newCommand.addAll(existingCommand);
        return newCommand;
    }

    @Override
    public List<String> getLaunchCommandPrefix(String workerId) {

        validReserveResource(workerId);

        StringBuilder sb = new StringBuilder();

        sb.append(this.conf.get(DaemonConfig.STORM_CGROUP_CGEXEC_CMD)).append(" -g ");

        Iterator<SubSystemType> it = this.subSystems.iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
            if (it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }
        sb.append(getWorkerGroup(workerId, subSystems.get(0)).getName());
        List<String> newCommand = new ArrayList<String>();
        newCommand.addAll(Arrays.asList(sb.toString().split(" ")));
        return newCommand;
    }

    private void validReserveResource(String workerId) {
        for (SubSystemType subSystem : subSystems) {
            CgroupCommon workerGroup = getWorkerGroup(workerId, subSystem);

            if (!this.rootCgroups.get(subSystem).getChildren().contains(workerGroup)) {
                throw new RuntimeException(
                        "cgroup " + workerGroup + " doesn't exist! Need to reserve resources for worker first!");
            }
        }
    }

    @Override
    public Set<Long> getRunningPids(String workerId) throws IOException {
        SubSystemType indexSubSystem = this.subSystems.get(0);
        CgroupCommon workerGroup = getWorkerGroup(workerId, indexSubSystem);
        if (!this.rootCgroups.get(indexSubSystem).getChildren().contains(workerGroup)) {
            LOG.warn("cgroup {} doesn't exist!", workerGroup);
            return Collections.emptySet();
        }
        return workerGroup.getPids();
    }

    @Override
    public long getMemoryUsage(String workerId) throws IOException {
        CgroupCommon workerGroup = getWorkerGroup(workerId, SubSystemType.memory);
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
        return memCore.getPhysicalUsage();
    }

    private static final Pattern MEMINFO_PATTERN = Pattern.compile("^([^:\\s]+):\\s*([0-9]+)\\s*kB$");

    static long getMemInfoFreeMb() throws IOException {
        //MemFree:        14367072 kB
        //Buffers:          536512 kB
        //Cached:          1192096 kB
        // MemFree + Buffers + Cached
        long memFree = 0;
        long buffers = 0;
        long cached = 0;
        try (BufferedReader in = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line = null;
            while ((line = in.readLine()) != null) {
                Matcher match = MEMINFO_PATTERN.matcher(line);
                if (match.matches()) {
                    String tag = match.group(1);
                    if (tag.equalsIgnoreCase("MemFree")) {
                        memFree = Long.parseLong(match.group(2));
                    } else if (tag.equalsIgnoreCase("Buffers")) {
                        buffers = Long.parseLong(match.group(2));
                    } else if (tag.equalsIgnoreCase("Cached")) {
                        cached = Long.parseLong(match.group(2));
                    }
                }
            }
        }
        return (memFree + buffers + cached) / 1024;
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;
        try {
            MemoryCore memRoot = (MemoryCore) rootCgroups.get(SubSystemType.memory).getCores().get(SubSystemType.memory);
            if (memRoot != null) {
                //For cgroups no limit is max long.
                long limit = memRoot.getPhysicalUsageLimit();
                long used = memRoot.getMaxPhysicalUsage();
                rootCgroupLimitFree = (limit - used) / 1024 / 1024;
            }
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        return Long.min(rootCgroupLimitFree, getMemInfoFreeMb());
    }
}
