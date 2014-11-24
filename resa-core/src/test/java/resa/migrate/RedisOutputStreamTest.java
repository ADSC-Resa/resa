package resa.migrate;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RedisOutputStreamTest {

    @Test
    public void testWrite() throws Exception {

        String host = "192.168.0.30";
        int port = 6379;
        String queueName = "tom-test";
        boolean overwrite = true;
        int chunkSize = 4;


        String testOutput = "aaaaabbbbbcccccdddddeeeeefffff";
        byte[] bTestOutput = testOutput.getBytes();

        try (RedisOutputStream ros = new RedisOutputStream(host, port, queueName, overwrite, chunkSize)) {
            ros.write(bTestOutput);
        }
    }

    @Test
    public void writeBigFile() {
        String host = "192.168.0.30";
        int port = 6379;
        String queueName = "test-data";
        try (RedisOutputStream ros = new RedisOutputStream(host, port, queueName, true, 10485760)) {
            Files.copy(Paths.get("/tmp/task-019.data"), ros);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}