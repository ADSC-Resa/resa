package resa.util;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import resa.metrics.ResaMetricsCollector;

import java.util.Map;

/**
 * Created by ding on 14-4-26.
 */
public class ResaConfig extends Config {

    public static final String TRACE_COMP_QUEUE = "topology.queue.trace";

    public static final String COMP_QUEUE_SAMPLE_RATE = "resa.comp.queue.sample.rate";

    public static final String COMP_SAMPLE_RATE = "resa.comp.sample.rate";

    public static final String MAX_EXECUTORS_PER_WORKER = "resa.topology.max.executor.per.worker";

    public static final String ZK_ROOT_PATH = "resa.scheduler.zk.root";

    public static final String REBALANCE_WAITING_SECS = "resa.topology.rebalance.waiting.secs";

    public static final String ALLOWED_EXECUTOR_NUM = "resa.topology.allowed.executor.num";

    public static final String OPTIMIZE_INTERVAL = "resa.optimize.interval.secs";

    public static final String ALLOC_CALC_CLASS = "resa.optimize.alloc.class";

    public static final String DECISION_MAKER_CLASS = "resa.scheduler.decision.class";

    private ResaConfig(boolean loadDefault) {
        if (loadDefault) {
            //read default.yaml & storm.yaml
            try {
                putAll(Utils.readStormConfig());
            } catch (Throwable e) {
            }
        }
        Map<String, Object> conf = Utils.findAndReadConfigFile("resa.yaml", false);
        if (conf != null) {
            putAll(conf);
        }
    }

    /**
     * Create a new Conf, then load default.yaml and storm.yaml
     *
     * @return
     */
    public static ResaConfig create() {
        return create(false);
    }

    /**
     * Create a new resa Conf
     *
     * @param loadDefault
     * @return
     */
    public static ResaConfig create(boolean loadDefault) {
        return new ResaConfig(loadDefault);
    }

    public void addOptimizeSupport() {
        registerMetricsConsumer(ResaMetricsCollector.class, 1);
    }
}
