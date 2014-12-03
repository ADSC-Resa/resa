package resa.evaluation.scheduler;

import backtype.storm.Config;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import resa.util.TopologyHelper;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-8-6.
 */
public class SchedulerTest {

    private CuratorFramework zk;
    private Map<String, Object> conf;

    @Before
    public void init() {
        conf = Utils.readStormConfig();
        conf.put(Config.NIMBUS_HOST, "192.168.0.30");
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("192.168.0.30"));
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
    }

    @Test
    public void assignment2InitState() throws Exception {
        String topoId = TopologyHelper.getTopologyId("test", conf);
        Map<String, Map<ExecutorDetails, String>> assignment = TopologyHelper.getAssignment(conf, topoId);
        String[] workers = assignment.values().stream().flatMap(m -> m.values().stream()).distinct().sorted()
                .toArray(String[]::new);
        Map<ExecutorDetails, String> compAssignment = assignment.get("data-loader");
        ExecutorDetails[] executors = compAssignment.keySet().stream()
                .sorted(Comparator.comparing(ExecutorDetails::getStartTask)).toArray(ExecutorDetails[]::new);
        int[] alloc = packAvg(executors.length, workers.length);
        Map<String, String> exe2WorkerSlot = new HashMap<>();
        int j = 0;
        for (int i = 0; i < alloc.length; i++) {
            for (int k = 0; k < alloc[i]; k++) {
                exe2WorkerSlot.put(executors[j].getStartTask() + "-" + executors[j].getEndTask(), workers[i]);
                j++;
            }
        }
        System.out.println(exe2WorkerSlot);
        String path = "/resa/" + topoId;
        if (zk.checkExists().forPath(path) != null) {
            zk.setData().forPath(path, Utils.serialize(new HashMap<>(Collections.singletonMap("assignment",
                    exe2WorkerSlot))));
        } else {
            zk.create().creatingParentsIfNeeded().forPath(path, Utils.serialize(
                    new HashMap<>(Collections.singletonMap("assignment", exe2WorkerSlot))));
        }
    }

    @Test
    public void changeAssignment() throws Exception {
        String topoId = TopologyHelper.getTopologyId("test", conf);
        Map<String, Map<ExecutorDetails, String>> assignment = TopologyHelper.getAssignment(conf, topoId);
        List<String> workers = assignment.values().stream().flatMap(m -> m.values().stream()).distinct()
                .collect(Collectors.toList());
        int numToMove = 9;
        Map<ExecutorDetails, String> compAssignment = assignment.get("data-loader");
        Set<ExecutorDetails> toMove = new HashSet<>();
        int[] moveCnt = packAvg(numToMove, workers.size());
        for (int i = 0; i < moveCnt.length; i++) {
            String worker = workers.get(i);
            compAssignment.entrySet().stream().filter(e -> e.getValue().equals(worker)).limit(moveCnt[i])
                    .map(e -> e.getKey()).forEach(toMove::add);
        }
        toMove.forEach(e -> {
            Collections.shuffle(workers);
            compAssignment.compute(e, (exe, worker) -> workers.stream().filter(w -> !w.equals(worker))
                    .findAny().get());
        });
        setAssignment(topoId, compAssignment);
    }

    private void setAssignment(String tid, Map<ExecutorDetails, String> compAssignment) throws Exception {
        Map<String, String> exe2WorkerSlot = new HashMap<>();
        String path = "/resa/" + tid;
        compAssignment.forEach((k, v) -> exe2WorkerSlot.put(k.getStartTask() + "-" + k.getEndTask(), v));
        zk.setData().forPath(path, Utils.serialize(new HashMap<>(Collections.singletonMap("assignment",
                exe2WorkerSlot))));
    }

    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }

    @Test
    public void testModifyExecutors() throws Exception {
        String path = "/resa/wc-2-1407306985";
        if (zk.checkExists().forPath(path) == null) {
            zk.create().forPath(path);
        }
        Map<String, Object> assignment = new HashMap<>();
        Map<String, String> exe2Comp = new HashMap<>();
        exe2Comp.put("5-6", "counter");
        exe2Comp.put("7-10", "counter");
        exe2Comp.put("11-14", "counter");
        exe2Comp.put("15-16", "counter");
        assignment.put("executors", exe2Comp);
        zk.setData().forPath(path, Utils.serialize(assignment));
    }

    @Test
    public void testModifyAssignment() throws Exception {
        String path = "/resa/wc-2-1407306985";
        if (zk.checkExists().forPath(path) == null) {
            zk.create().forPath(path);
        }
        Map<String, Object> assignment = new HashMap<>();
        Map<String, String> exe2Comp = new HashMap<>();
        exe2Comp.put("5-6", "counter");
        exe2Comp.put("7-10", "counter");
        exe2Comp.put("11-14", "counter");
        exe2Comp.put("15-16", "counter");
        assignment.put("executors", exe2Comp);
        Map<String, String> exe2WorkerSlot = new HashMap<>();
        exe2WorkerSlot.put("7-10", "yy08-Precision-T1650:6702");
        exe2WorkerSlot.put("5-6", "yy07-Precision-T1650:6701");
        assignment.put("assignment", exe2WorkerSlot);
        zk.setData().forPath(path, Utils.serialize(assignment));
    }

    @After
    public void cleanup() {
        zk.close();
    }

}
