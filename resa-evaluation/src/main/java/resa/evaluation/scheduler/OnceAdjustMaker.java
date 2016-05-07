package resa.evaluation.scheduler;

import backtype.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.AllocResult;
import resa.drs.DecisionMaker;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-7-11.
 */
public class OnceAdjustMaker implements DecisionMaker {

    private static final Logger LOG = LoggerFactory.getLogger(OnceAdjustMaker.class);
    private long startTime;
    private boolean adjust = false;
    private long minAdjustInterval;

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        startTime = now();
        minAdjustInterval = ConfigUtil.getInt(conf, "resa.adjust.min.sec", 300);
        LOG.info("OnceAdjustMaker is initialized, minAdjustInterval: " + minAdjustInterval);
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {
        if (!adjust && now() - startTime > minAdjustInterval) {
            adjust = true;
            Map<String, Integer> ret = new HashMap<>();
//            ret.put("input", 2);
//            ret.put("generator", 6);
//            ret.put("detector", 13);
//            ret.put("reporter", 3);
//            ret.put("image-input", 2);
//            ret.put("feat-ext", 10);
//            ret.put("matcher", 11);
//            ret.put("aggregater", 1);
//            ret.put("image-input", 2);
//            ret.put("feat-ext", 8);
//            ret.put("matcher", 8);
//            ret.put("aggregater", 1);

//            ret.put("image-input", 1);
//            ret.put("feat-ext", 10);
//            ret.put("matcher", 11);
//            ret.put("vd-aggregator", 1);

            ret.put("image-input", 1);
            ret.put("feat-ext", 8);
            ret.put("matcher", 8);
            ret.put("vd-aggregator", 1);

            ///for resaVLD
//            ret.put("tVLDSpout", 1);
//            ret.put("tVLDPatchGen", 1);
//            ret.put("tVLDPatchProc", 20);
//            ret.put("tVLDPatchAgg", 1);
//            ret.put("tVLDPatchDraw", 1);
//            ret.put("tVLDRedisFrameOut", 1);

            LOG.info("OnceAdjustMaker.make() is called with result allocation: " + ret);
            return ret;
        }
        return null;
    }

    private static long now() {
        return System.currentTimeMillis() / 1000;
    }

}
