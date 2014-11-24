package resa.evaluation.scheduler;

import backtype.storm.Config;
import backtype.storm.scheduler.Topologies;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import junit.framework.TestCase;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyListenerTest extends TestCase {

    private TopologyListener topologyListener;
    private Map<String, Object> conf = ResaConfig.create(true);
    private CuratorFramework zk;

    @Override
    protected void setUp() throws Exception {
        topologyListener = new TopologyListener(conf);
        conf.put(ResaConfig.ZK_ROOT_PATH, "/resa");
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
    }

    public void testSynTopologies() throws Exception {
        Topologies topologies = TopologyHelper.getTopologyDetails(conf);
        System.out.println("total topology count: " + topologies.getTopologies().size());
        topologyListener.synTopologies(topologies);
        Utils.sleep(10000);

        String topoId = TopologyHelper.getTopologyId("wc", conf);
        Map<String, Integer> newAssignment = Collections.singletonMap("split", 5);
        byte[] data = Utils.serialize(new HashMap<>(newAssignment));
        if (zk.checkExists().forPath("/resa/" + topoId) == null) {
            zk.create().creatingParentsIfNeeded().forPath("/resa/" + topoId, data);
            System.out.println("Set assignment");
        }
        Utils.sleep(1000);
        topologyListener.synTopologies(topologies);
        System.out.println("Reset assignment");
        zk.setData().forPath("/resa/" + topoId, data);
        Utils.sleep(31000);

        TopologyHelper.killTopology("wc", 0, conf);
        Utils.sleep(10000);
        topologies = TopologyHelper.getTopologyDetails(conf);
        System.out.println("total topology count: " + topologies.getTopologies().size());
        topologyListener.synTopologies(topologies);
    }
}