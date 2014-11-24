package resa.migrate;

import junit.framework.TestCase;

public class RedisInputStreamTest extends TestCase {

    public void testCreate() throws Exception {

        String host = "192.168.0.30";
        int port = 6379;
        String queueName = "tom-test";

        byte[] bTestInput = new byte[10000];

        try(RedisInputStream ris = RedisInputStream.create(host, port, queueName)){

            int ret = ris.read(bTestInput);

            char[] inputChar = new char[bTestInput.length];

            for(int i = 0; i < inputChar.length; i ++){
               inputChar[i] = (char)bTestInput[i];
            }

            String testInput = String.valueOf(inputChar);

            System.out.println(testInput);
        }


    }

    public void testRead() throws Exception {

    }
}