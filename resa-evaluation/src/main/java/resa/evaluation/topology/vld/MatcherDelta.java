package resa.evaluation.topology.vld;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static resa.evaluation.topology.vld.Constant.*;

/**
 * Created by ding on 14-7-3.
 *
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class MatcherDelta extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(MatcherDelta.class);

    private static final int[] EMPTY_MATCH = new int[0];
    private Map<byte[], int[]> featDesc2Image;
    private OutputCollector collector;
    private double distThreshold;

    @Override

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        featDesc2Image = new HashMap<>();
        loadFullIndex();
        this.collector = collector;
        distThreshold = ConfigUtil.getDouble(stormConf, CONF_FEAT_DIST_THRESHOLD, 100);
    }

    private void loadIndex(int index, int totalPieces) {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/index.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || count++ % totalPieces != index) {
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(line);
                String[] tmp = StringUtils.split(tokenizer.nextToken(), ',');
                byte[] feat = new byte[tmp.length];
                for (int i = 0; i < feat.length; i++) {
                    feat[i] = (byte) (((int) Double.parseDouble(tmp[i])) & 0xFF);
                }
                int[] images = Stream.of(StringUtils.split(tokenizer.nextToken(), ',')).mapToInt(Integer::parseInt)
                        .toArray();
                featDesc2Image.put(feat, images);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("taskIndex=" + index + ", totalIndexPieces=" + totalPieces + ", totalIndexEntry=" + count + ", load="
                + featDesc2Image.size());
    }

    private void loadFullIndex() {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/index.txt")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                String[] tmp = StringUtils.split(tokenizer.nextToken(), ',');
                byte[] feat = new byte[tmp.length];
                for (int i = 0; i < feat.length; i++) {
                    feat[i] = (byte) (((int) Double.parseDouble(tmp[i])) & 0xFF);
                }
                int[] images = Stream.of(StringUtils.split(tokenizer.nextToken(), ',')).mapToInt(Integer::parseInt)
                        .toArray();
                featDesc2Image.put(feat, images);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("loadFullIndex, load=" + featDesc2Image.size());
    }

    @Override
    public void execute(Tuple input) {
        String frameId = input.getStringByField(FIELD_FRAME_ID);
        List<byte[]> desc = (List<byte[]>) input.getValueByField(FIELD_FEATURE_DESC);
        int patchCount = input.getIntegerByField(FIELD_PATCH_COUNT);

        Map<Integer, Long> image2Freq = desc.stream().flatMap(imgDesc -> findMatches(imgDesc).stream())
                .flatMap(imgList -> IntStream.of(imgList).boxed())
                .collect(Collectors.groupingBy(i -> i, Collectors.counting()));
        int[] matches = image2Freq.isEmpty() ? EMPTY_MATCH : new int[image2Freq.size() * 2];
        int i = 0;
        for (Map.Entry<Integer, Long> m : image2Freq.entrySet()) {
            matches[i++] = m.getKey();
            matches[i++] = m.getValue().intValue();
        }
        collector.emit(STREAM_MATCH_IMAGES, input, new Values(frameId, matches, patchCount, desc.size()));
        collector.ack(input);
    }

    private List<int[]> findMatches(byte[] desc) {
        List<int[]> matches = new ArrayList<>();
        for (Map.Entry<byte[], int[]> e : featDesc2Image.entrySet()) {
            double d = distance(e.getKey(), desc);
            if (d < distThreshold) {
                matches.add(e.getValue());
            }
        }
        return matches;
    }

    private double distance(byte[] v1, byte[] v2) {
        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            double d = (v1[i] & 0xFF) - (v2[1] & 0xFF);
            sum += d * d;
        }
        return Math.sqrt(sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_MATCH_IMAGES, new Fields(FIELD_FRAME_ID, FIELD_MATCH_IMAGES, FIELD_PATCH_COUNT, FIELD_FEATURE_CNT));
    }

}
