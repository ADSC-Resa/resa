package resa.util;

import redis.clients.jedis.Jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * This class is not thread-safe
 * Created by ding on 14-5-29.
 */
public class RedisQueueIterable implements Iterable<String>, Closeable {

    private String host;
    private int port;
    private String queue;
    private Jedis jedis;
    private final long end;
    private final long start;
    private final int bufferSize = 500;

    public RedisQueueIterable(String host, int port, String queue) {
        this(host, port, queue, Long.MAX_VALUE);
    }

    public RedisQueueIterable(String host, int port, String queue, long maxCount) {
        this(host, port, queue, 0, maxCount);
    }

    public RedisQueueIterable(String host, int port, String queue, long start, long maxCount) {
        this.host = host;
        this.port = port;
        this.queue = queue;
        this.end = maxCount == Long.MAX_VALUE ? Long.MAX_VALUE : start + maxCount;
        this.start = start;
    }

    private synchronized String[] fetchNextRange(long from) {
        if (from < end) {
            long bound = Math.min(end, from + bufferSize) - 1;
            try {
                List<String> nextRange = getJedis().lrange(queue, from, bound);
                if (!nextRange.isEmpty()) {
                    return nextRange.toArray(new String[nextRange.size()]);
                }
            } catch (Exception e) {
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        disconnect();
    }

    private class DataIter implements Iterator<String> {

        private long count = 0;
        private String[] cache = null;
        private int pos = 0;

        @Override
        public boolean hasNext() {
            if (cache == null || pos == cache.length) {
                cache = fetchNextRange(start + count);
                if (cache == null) {
                    return false;
                }
                pos = 0;
            }
            return true;
        }

        @Override
        public String next() {
            count++;
            return cache[pos++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private Jedis getJedis() {
        if (jedis != null) {
            return jedis;
        }
        //try connect to redis server
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
        }
        return jedis;
    }

    private void disconnect() {
        if (jedis != null) {
            try {
                jedis.disconnect();
            } catch (Exception e) {
            }
            jedis = null;
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new DataIter();
    }

}
