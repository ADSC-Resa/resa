package resa.evaluation.simulate;

import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-7-29.
 */
public class DataStat {

    public static void main(String[] args) throws Exception {
        Map stormConf = Utils.readStormConfig();
        CuratorFramework zk = Utils.newCuratorStarted(stormConf, (List<String>) stormConf.get("storm.zookeeper.servers"),
                stormConf.get("storm.zookeeper.port"));
        String prefix = args[0];
        int numTask = Integer.parseInt(args[1]);
        int[] costs = new int[numTask];
        for (int i = 0; i < numTask; i++) {
            costs[i] = Integer.parseInt(new String(zk.getData().forPath(prefix + i)));
        }
        long total = IntStream.of(costs).sum() / 1000;
        System.out.println("total cost: " + total + "s");
        System.out.println("avg cost: " + (total / numTask) + "s");
        System.out.println("max cost: " + IntStream.of(costs).max().getAsInt() / 1000 + "s");
        zk.close();
    }

}
