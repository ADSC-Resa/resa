package resa.evaluation.scheduler.plan;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZKUtil;
import org.junit.Before;
import org.junit.Test;
import resa.evaluation.migrate.ConsistentHashing;
import resa.util.TopologyHelper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-8-12.
 */
public class MigrateSimulateForCH {

    private CuratorFramework zk;
    private Map<String, Object> conf;
    private int[] allocations;
    private List<String> slots;
    private double[] dataSizes;
    private ExecutorDetails[] executors;
    private String topoName = "fpt";

    @Before
    public void init() throws Exception {
        conf = Utils.readStormConfig();
        conf.put(Config.NIMBUS_HOST, "192.168.0.30");
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("192.168.0.30"));
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
        allocations = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/ch-migrate.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty())
                .mapToInt(Integer::parseInt).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/fp-data-sizes-064-70.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        slots = getSlots();
        String topoId = TopologyHelper.getTopologyId(topoName, conf);
        Map<String, Map<ExecutorDetails, String>> assignment = TopologyHelper.getAssignment(conf, topoId);
        Map<ExecutorDetails, String> compAssignment = assignment.get("detector");
        executors = compAssignment.keySet().stream().sorted(Comparator.comparing(ExecutorDetails::getStartTask))
                .toArray(ExecutorDetails[]::new);
    }

    @Test
    public void deleteZkNode() throws Exception {
        ZKUtil.deleteRecursive(zk.getZookeeperClient().getZooKeeper(), "/resa");
    }

    @Test
    public void run() throws Exception {
        int state = 0;
        List<int[]> from = new ArrayList<>();
        List<int[]> to = new ArrayList<>();
        String[] workers = slots.stream().limit(allocations[state]).toArray(String[]::new);
        assignment2State(from, workers);
        doRebalance(topoName, allocations[state]);
        System.out.println("Init state, curr alloc=" + allocations[state]);
        Scanner scanner = new Scanner(System.in);
        String line;
        int count = 0;
        while ((line = scanner.nextLine()) != null) {
            if (line.equals("c")) {
                count++;
                System.out.println("run " + count + ", from " + allocations[state] + " to " + allocations[state + 1]);
                System.out.println("old workers:" + Arrays.toString(workers));
                workers = migrateTo(from, workers, to, allocations[state + 1]);
                System.out.println("new workers:" + Arrays.toString(workers));
                assignment2State(to, workers);
                doRebalance(topoName, allocations[state + 1]);
                state++;
                from.clear();
                from.addAll(to);
                to.clear();
            }
        }
    }

    private String[] migrateTo(List<int[]> from, String[] workers, List<int[]> to, int toNumWorkers) {
        ConsistentHashing.Result res = ConsistentHashing.calc(dataSizes, from, toNumWorkers);
        to.addAll(res.tasks);
        String[] newWorkers = new String[toNumWorkers];
        Set<String> selected = new HashSet<>();
        int[][] map = res.matching;
        for (int i = 0; i < map.length; i++) {
            newWorkers[map[i][0]] = workers[map[i][1]];
            selected.add(newWorkers[map[i][0]]);
        }
        for (int i = 0; i < newWorkers.length; i++) {
            if (newWorkers[i] == null) {
                for (String w : slots) {
                    if (!selected.contains(w)) {
                        selected.add(w);
                        newWorkers[i] = w;
                        break;
                    }
                }
            }
        }
        return newWorkers;
    }

    public void assignment2State(List<int[]> alloc, String[] workers) throws Exception {
        Map<String, String> exe2WorkerSlot = new HashMap<>();
        for (int i = 0; i < alloc.size(); i++) {
            for (int task : alloc.get(i)) {
                exe2WorkerSlot.put(executors[task].getStartTask() + "-" + executors[task].getEndTask(), workers[i]);
            }
        }
//        System.out.println(exe2WorkerSlot);
        String path = "/resa/" + TopologyHelper.getTopologyId(topoName, conf);
        if (zk.checkExists().forPath(path) != null) {
            zk.setData().forPath(path, Utils.serialize(new HashMap<>(Collections.singletonMap("assignment",
                    exe2WorkerSlot))));
        } else {
            zk.create().creatingParentsIfNeeded().forPath(path, Utils.serialize(
                    new HashMap<>(Collections.singletonMap("assignment", exe2WorkerSlot))));
        }
    }


    private List<String> getSlots() throws Exception {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client nimbus = nimbusClient.getClient();
        List<String> slots = nimbus.getClusterInfo().get_supervisors().stream()
                .flatMap(s -> Arrays.asList(s.get_host() + ":6700", s.get_host() + ":6701").stream())
                .collect(Collectors.toList());
        Collections.shuffle(slots);
        System.out.println(slots.size());
        return slots;
    }

    private void doRebalance(String topoName, int numWorkers) throws Exception {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client nimbus = nimbusClient.getClient();
        RebalanceOptions options = new RebalanceOptions();
        options.set_num_workers(numWorkers);
        options.set_wait_secs(1);
        System.out.println("Reassigning to " + numWorkers + " workers");
        nimbus.rebalance(topoName, options);
    }

    public static void main(String[] args) throws Exception {
        MigrateSimulateForCH simulate = new MigrateSimulateForCH();
        simulate.init();
        simulate.run();
    }

}
