package resa.evaluation.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import resa.optimize.AllocResult;
import resa.drs.DecisionMaker;
import resa.util.ConfigUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-6-26.
 */
public class TracedDecisionMaker implements DecisionMaker {

    private static final Logger LOG = LoggerFactory.getLogger(TracedDecisionMaker.class);

    private Jedis jedis;
    private String queueName;
    private ObjectMapper objMapper = new ObjectMapper();

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        jedis = new Jedis((String) conf.get("redis.host"), ConfigUtil.getInt(conf, "redis.port", 6379));
        queueName = conf.getOrDefault(Config.STORM_ID, "") + "-decision-data";
        LOG.info("dump to queue " + queueName);
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", newAllocResult.status.name());
        if (newAllocResult.minReqOptAllocation != null) {
            data.put("minReq", newAllocResult.minReqOptAllocation);
        }
        if (newAllocResult.currOptAllocation != null) {
            data.put("optAlloc", newAllocResult.currOptAllocation);
        }
        try {
            jedis.rpush(queueName, objMapper.writeValueAsString(data));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return currAlloc;
    }

}
