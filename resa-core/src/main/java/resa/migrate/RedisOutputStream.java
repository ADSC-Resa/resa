package resa.migrate;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by ding on 14-7-15.
 */
public class RedisOutputStream extends OutputStream {

    private int chunkSize;
    private Jedis jedis;
    private byte[] buf;
    private int pos = 0;
    private byte[] queueName;

    public RedisOutputStream(String host, int port, String queueName) {
        this(host, port, queueName, true);
    }

    public RedisOutputStream(String host, int port, String queueName, boolean overwrite) {
        this(host, port, queueName, overwrite, 1 << 20);
    }

    public RedisOutputStream(String host, int port, String queueName, boolean overwrite, int chunkSize) {
        jedis = new Jedis(host, port);
        buf = new byte[chunkSize];
        this.chunkSize = chunkSize;
        this.queueName = queueName.getBytes();
        if (overwrite) {
            jedis.del(queueName);
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (pos == buf.length) {
            finishChunk(false);
        }
        buf[pos++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int retain = len;
        while (retain > 0) {
            int copySize = Math.min(retain, buf.length - pos);
            System.arraycopy(b, off + (len - retain), buf, pos, copySize);
            retain -= copySize;
            pos += copySize;
            if (pos == buf.length) {
                finishChunk(false);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        finishChunk(false);
    }

    private void finishChunk(boolean close) {
        if (pos != 0) {
            byte[] output = pos == buf.length ? buf : Arrays.copyOf(buf, pos);
            jedis.rpush(queueName, output);
            pos = 0;
            buf = close ? null : new byte[chunkSize];
        }
    }

    @Override
    public void close() throws IOException {
        finishChunk(true);
        jedis.disconnect();
    }
}
