package resa.evaluation.util;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import resa.optimize.AggResultCalculator;
import resa.optimize.AllocCalculator;
import resa.optimize.GeneralAllocCalculator;
import resa.optimize.SimpleGeneralAllocCalculator;
import resa.util.ResaConfig;
import resa.util.ResaUtils;
import resa.util.TopologyHelper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.ALLOC_CALC_CLASS;

/**
 * Created by Tom.fu on 5/5/2014.
 */
public class ResaMetricAnalyzer {

    //There are three types of output metric information.
    // "drs.alloc->{\"status\":\"FEASIBLE\",\"minReqOptAllocation\":{\"2Path-BoltA-NotP\":1,\"2Path-BoltA-P\":1,\"2Path-BoltB\":1,\"2Path-Spout\":1},
    //"topology.info"->
    //"task.2Path-Spout.16->{\    private Map<String, Object> conf = ResaConfig.create(true);

    private Map<String, Object> conf = ResaConfig.create(true);
    public static void main(String[] args) {
        System.out.println("ResaMetricAnalyzer based on ResaDataSource");
        try {
            String topName = args[0];
            String metricQueue = args[1];
            long sleepTime = Long.parseLong(args[2]);
            int maxAllowedExecutors = Integer.parseInt(args[3]);
            double qos = Double.parseDouble(args[4]);
            int historySize = Integer.parseInt(args[5]);
            System.out.println("Topology name: " + topName + ", metricQueue: " + metricQueue
                    + ", sleepTime: " + sleepTime + ", maxAllowed: " + maxAllowedExecutors + ", qos: " + qos);
            ResaMetricAnalyzer rt = new ResaMetricAnalyzer();
            rt.testMakeUsingTopologyHelperForkTopology(topName, metricQueue, sleepTime, maxAllowedExecutors, qos, historySize);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void testMakeUsingTopologyHelperForkTopology(String topoName, String metricQueue,
                                                        long sleepTime, int allewedExecutorNum, double qos, int historySize) throws Exception {

        conf.put(Config.NIMBUS_HOST, "192.168.0.31");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.TOPOLOGY_DEBUG, true);

        conf.put("resa.opt.smd.qos.ms", qos);
        conf.put("resa.opt.win.history.size", historySize);
        conf.put("resa.opt.win.history.size.ignore", 0);
        conf.put("resa.comp.sample.rate", 1.0);

        conf.put(ResaConfig.ALLOWED_EXECUTOR_NUM, allewedExecutorNum);

        String host = "192.168.0.31";
        int port = 6379;
        int maxLen = 5000;

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client nimbus = nimbusClient.getClient();

        String topoId = TopologyHelper.getTopologyId(nimbus, topoName);
        TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);

        Map<String, Integer> currAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                .collect(Collectors.groupingBy(e -> e.get_component_id(),
                        Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

//        AllocCalculator smdm = new SimpleGeneralAllocCalculator();
        AllocCalculator smdm = new GeneralAllocCalculator();

        smdm.init(conf, currAllocation, nimbus.getUserTopology(topoId));

        for (int i = 0; i < 10000; i++) {
            Utils.sleep(sleepTime);

            topoInfo = nimbus.getTopologyInfo(topoId);
            Map<String, Integer> updatedAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                    .collect(Collectors.groupingBy(e -> e.get_component_id(),
                            Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

            Map<String, List<ExecutorDetails>> comp2Executors = TopologyHelper.getTopologyExecutors(topoName, conf)
                    .entrySet().stream().filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            AggResultCalculator resultCalculator = new AggResultCalculator(
                    ResaDataSource.readData(host, port, metricQueue, maxLen), comp2Executors, nimbus.getUserTopology(topoId));
            resultCalculator.calCMVStat();

            System.out.println("-------------Report on: " + System.currentTimeMillis() + "------------------------------");
            if (currAllocation.equals(updatedAllocation)) {
                System.out.println(currAllocation + "-->" + smdm.calc(resultCalculator.getComp2ExecutorResults(), allewedExecutorNum));
            } else {
                currAllocation = updatedAllocation;
                smdm.allocationChanged(currAllocation);
                ResaDataSource.clearQueue(host, port, metricQueue);
                System.out.println("Allocation updated to " + currAllocation);
            }
        }
    }
}
