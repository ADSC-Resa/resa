package resa.evaluation.scheduler.plan;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZKUtil;
import org.junit.Before;
import org.junit.Test;
import resa.scheduler.plan.KuhnMunkres;
import resa.scheduler.plan.PackCalculator;
import resa.util.TopologyHelper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-8-12.
 */
public class MigrateSimulate {

    private CuratorFramework zk;
    private Map<String, Object> conf;
    private List<int[]> allocations;
    private List<String> slots;
    private double[] dataSizes;
    private KuhnMunkres km;
    private ExecutorDetails[] executors;
    private String topoName = "fpt";

    @Before
    public void init() throws Exception {
        conf = Utils.readStormConfig();
        conf.put(Config.NIMBUS_HOST, "192.168.0.30");
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("192.168.0.30"));
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
        allocations = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/local-migrate.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(s -> Stream.of(s.split(",")).mapToInt(Integer::parseInt).toArray())
                .collect(Collectors.toList());
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/fp-data-sizes-064-70.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        slots = getSlots();
        km = new KuhnMunkres(dataSizes.length);
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
        int state = 2;
        String[] workers = slots.stream().limit(allocations.get(state).length).toArray(String[]::new);
        assignment2State(allocations.get(state), workers);
        doRebalance(topoName, allocations.get(state).length);
        System.out.println("Init state, curr alloc=" + Arrays.toString(allocations.get(state)));
        Scanner scanner = new Scanner(System.in);
        String line;
        int count = 0;
        while ((line = scanner.nextLine()) != null) {
            if (line.equals("c")) {
                count++;
                System.out.println("run " + count + ", curr alloc=" + Arrays.toString(allocations.get(state)) +
                        ", target alloc=" + Arrays.toString(allocations.get(state + 1)));
                System.out.println("old workers:" + Arrays.toString(workers));
                workers = migrateTo(allocations.get(state), workers, allocations.get(state + 1));
                System.out.println("new workers:" + Arrays.toString(workers));
                assignment2State(allocations.get(state + 1), workers);
                doRebalance(topoName, allocations.get(state + 1).length);
                state++;
            }
        }
    }

    private String[] migrateTo(int[] from, String[] workers, int[] to) {
        PackCalculator.Range[] pack1 = PackCalculator.convertPack(from);
        PackCalculator.Range[] pack2 = PackCalculator.convertPack(to);
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        int[][] map = km.getMaxBipartie(weights, new double[1]);
        String[] newWorkers = new String[to.length];
        Set<String> selected = new HashSet<>();
        for (int i = 0; i < map.length; i++) {
            newWorkers[map[i][1]] = workers[map[i][0]];
            selected.add(newWorkers[map[i][1]]);
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

    private double overlap(PackCalculator.Range r1, PackCalculator.Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

    public void assignment2State(int[] alloc, String[] workers) throws Exception {
        Map<String, String> exe2WorkerSlot = new HashMap<>();
        int j = 0;
        for (int i = 0; i < alloc.length; i++) {
            for (int k = 0; k < alloc[i]; k++) {
                exe2WorkerSlot.put(executors[j].getStartTask() + "-" + executors[j].getEndTask(), workers[i]);
                j++;
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
        MigrateSimulate simulate = new MigrateSimulate();
        simulate.init();
        simulate.run();
    }

}
