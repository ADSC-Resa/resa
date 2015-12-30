package resa.evaluation.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift7.TException;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Class responsible for watch topology's allocation, and calc a decision whether change of allocation
 * should be take effect.
 *
 * @author Troy Ding
 */
public class TopologyListener {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyListener.class);

    public static class AllocationContext {
        private long lastRequest;
        private long lastRebalance;
        private Map<String, Integer> compExecutors;
        private TopologyDetails topologyDetails;

        public AllocationContext setCompExecutors(Map<String, Integer> compExecutors) {
            this.compExecutors = compExecutors;
            return this;
        }

        public AllocationContext setTopologyDetails(TopologyDetails topologyDetails) {
            this.topologyDetails = topologyDetails;
            return this;
        }

        public AllocationContext(TopologyDetails topologyDetails, Map<String, Integer> compExecutors) {
            this.lastRequest = System.currentTimeMillis();
            this.compExecutors = compExecutors;
            this.topologyDetails = topologyDetails;
        }

        public AllocationContext updateLastRebalance() {
            lastRequest = (lastRebalance = System.currentTimeMillis());
            return this;
        }

        public AllocationContext updateLastRequest() {
            lastRequest = System.currentTimeMillis();
            return this;
        }
    }

    private CuratorFramework zk;
    private String rootPath;
    private Map<String, AllocationContext> watchingTopologies = new ConcurrentHashMap<>();
    private Nimbus.Client nimbus;
    private final int maxExecutorsPerWorker;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final int rebalanceWaitingSecs;

    public TopologyListener(Map<String, Object> conf) {
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT), (ZookeeperAuthInfo)null);
        rootPath = (String) conf.getOrDefault(ResaConfig.ZK_ROOT_PATH, "/resa");
        checkZKNode();
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, ResaConfig.MAX_EXECUTORS_PER_WORKER, 10);
        rebalanceWaitingSecs = ConfigUtil.getInt(conf, ResaConfig.REBALANCE_WAITING_SECS, -1);
    }

    private void checkZKNode() {
        try {
            if (zk.checkExists().forPath(rootPath) == null) {
                zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Check root path failed: " + rootPath, e);
        }
    }

    /**
     * Synchronize the watching topologies with alive topologies in the cluster. Dead topology will
     * be removed from the watching list.
     *
     * @param topologies alive topologies
     */
    public void synTopologies(Topologies topologies) {
        Set<String> aliveTopoIds = topologies.getTopologies().stream().map(TopologyDetails::getId)
                .collect(Collectors.toSet());
        // remove topologies that dead
        watchingTopologies.keySet().retainAll(aliveTopoIds);
        topologies.getTopologies().stream().forEach(this::addOrUpdateTopology);
    }

    /**
     * Add a topology to the watching list or update corresponding TopologyDetails object
     * if this topology is under watching.
     *
     * @param topoDetails
     */
    public void addOrUpdateTopology(TopologyDetails topoDetails) {
        String topoId = topoDetails.getId();
        // For a new joined topology, set a new watcher on it and add it to watching list
        // For a watching topology, update its running detail
        watchingTopologies.compute(topoId, (topologyId, context) ->
                (context == null ? watchTopology(topoDetails) : context.setTopologyDetails(topoDetails)));
    }

    /* add a watcher on zk to get a notification when a new assignment is set */
    private AllocationContext watchTopology(TopologyDetails topoDetails) {
        // get current assignment and set a watcher on the zk node
        Map<String, Integer> compExecutors = getCompExecutorsAndWatch(topoDetails.getId());
        if (compExecutors == null) {
            return null;
        }
        LOG.info("Begin to watching topology " + topoDetails.getId());
        return new AllocationContext(topoDetails, compExecutors);
    }

    /**
     * TODO: need validate the correctness of the modification
     * @param topoId
     * @return
     */
    private Map<String, Integer> getCompExecutorsAndWatch(String topoId) {
        String path = rootPath + '/' + topoId;
        try {
            byte[] data = zk.getData().usingWatcher(new TopologyWatcher(topoId)).forPath(path);
            if (data != null) {
                //return (Map<String, Integer>) Utils.deserialize(data);
                return (Map<String, Integer>) Utils.deserialize(data, Map.class);
            }
        } catch (Exception e) {
        }
        return null;
    }

    /* zk watcher */
    private class TopologyWatcher implements Watcher {

        private String topoId;

        private TopologyWatcher(String topoId) {
            this.topoId = topoId;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                Map<String, Integer> newCompExecutors = getCompExecutorsAndWatch(topoId);
                if (newCompExecutors != null) {
                    watchingTopologies.computeIfPresent(topoId, (topoId, context) -> {
                        context.setCompExecutors(newCompExecutors);
                        // do this in the other thread, so that watchingTopologies can't block
                        threadPool.submit(() -> tryRebalance(topoId, context));
                        return context;
                    });
                }
                LOG.info("A new assignment is set for topology " + topoId);
            } else {
                // maybe something wrong with zk connection, force re-watch
                watchingTopologies.remove(topoId);
                LOG.warn("Receive a exception event for topology " + topoId + ", event type is "
                        + event.getType() + ", state is " + event.getState());
            }
        }

    }

    private void tryRebalance(String topoId, AllocationContext context) {
        if (needRebalance(context)) {
            LOG.info("Trying rebalance for topology " + topoId);
            requestRebalance(topoId, context);
        } else {
            LOG.info("Request rebalance denied for topology " + topoId);
            context.updateLastRequest();
        }
    }

    /**
     * check whether a rebalance operation on the specified context is permitted
     */
    protected boolean needRebalance(AllocationContext context) {
        if (context.compExecutors.isEmpty()) {
            return false;
        }
        return !context.compExecutors.equals(TopologyHelper.getComponentExecutorCount(context.topologyDetails));
    }

    /* Send rebalance request to nimbus */
    private void requestRebalance(String topoId, AllocationContext context) {
        int totolNumExecutors = context.compExecutors.values().stream().mapToInt(i -> i).sum();
        int numWorkers = totolNumExecutors / maxExecutorsPerWorker;
        if (totolNumExecutors % maxExecutorsPerWorker > (int) (maxExecutorsPerWorker / 2)) {
            numWorkers++;
        }
        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options
        options.set_num_workers(numWorkers);
        options.set_num_executors(context.compExecutors);
        if (rebalanceWaitingSecs >= 0) {
            options.set_wait_secs(rebalanceWaitingSecs);
        }
        try {
            nimbus.rebalance(TopologyHelper.topologyId2Name(topoId), options);
            LOG.info("do rebalance successfully for topology " + topoId);
            context.updateLastRebalance();
        } catch (NotAliveException e) {
            watchingTopologies.remove(topoId);
            LOG.warn("topology is not exist, maybe killed, remove from waiting list: " + topoId);
        } catch (InvalidTopologyException e) {
            watchingTopologies.remove(topoId);
            LOG.warn("topology is not exist, maybe killed, remove from waiting list: " + topoId);
        } catch (TException e) {
            LOG.warn("do rebalance failed for topology " + topoId, e);
        }
    }

}