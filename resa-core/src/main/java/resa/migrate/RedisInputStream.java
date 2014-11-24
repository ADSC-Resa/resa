package resa.migrate;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by ding on 14-7-16.
 */
public class RedisInputStream extends InputStream {

    private Jedis jedis;
    private byte[] buf;
    private int pos = 0;
    private byte[] queueName;
    private int listPos = 0;
    private final int chunkCount;

    private RedisInputStream(Jedis jedis, String queueName) {
        this.jedis = jedis;
        this.queueName = queueName.getBytes();
        chunkCount = jedis.llen(queueName).intValue();
    }

    public static RedisInputStream create(String host, int port, String queueName) {
        try {
            Jedis jedis = new Jedis(host, port);
            return jedis.exists(queueName) ? new RedisInputStream(jedis, queueName) : null;
        } catch (Exception e) {
        }
        return null;
    }

    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[1];
        return read(bytes) > 0 ? (bytes[0] & 0xFF) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int remain = len;
        while (remain > 0) {
            if (buf == null) {
                if (listPos >= chunkCount) {
                    break;
                } else {
                    buf = jedis.lindex(this.queueName, listPos);
                    pos = 0;
                }
                listPos++;
            }
            int size = Math.min(remain, buf.length - pos);
            System.arraycopy(buf, pos, b, off + (len - remain), size);
            remain -= size;
            pos += size;
            if (pos == buf.length) {
                buf = null;
            }
        }
        return remain == len ? -1 : len - remain;
    }

    @Override
    public void close() throws IOException {
        jedis.disconnect();
        buf = null;
    }
}
