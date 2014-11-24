package resa.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Created by ding on 14-7-31.
 */
public class FileServer extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(FileServer.class);

    private final int port;
    private ServerSocket serverSocket;
    private final Path root;
    private volatile boolean stop = false;

    public FileServer(int port, String base) {
        super("File server");
        setDaemon(true);
        root = Paths.get(base);
        this.port = port;
    }

    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started at port " + port);
        } catch (Exception e) {
            System.err.println("Port " + port + " already in use.");
            return;
        }
        while (!stop) {
            try {
                Socket clientSocket = serverSocket.accept();
                // System.out.println("Accepted connection : " + clientSocket);
                Thread t = new Thread(new Connection(clientSocket));
                t.start();
            } catch (Exception e) {
                System.err.println("Error in connection attempt.");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        FileServer server = new FileServer(Integer.parseInt(args[0]), args[1]);
        server.start();
        server.join();
    }

    private class Connection implements Runnable {
        private Socket clientSocket;

        public Connection(Socket client) {
            this.clientSocket = client;
        }

        @Override
        public void run() {
            try (DataInputStream in = new DataInputStream(clientSocket.getInputStream())) {
                String clientSelection = in.readUTF();
                switch (clientSelection) {
                    case "write":
                        receiveFile(in, in.readUTF());
                        break;
                    case "read":
                        String outGoingFileName = in.readUTF();
                        sendFile(outGoingFileName);
                        break;
                    default:
                        System.out.println("Incorrect command received.");
                        break;
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                }
            }
        }

        public void receiveFile(DataInputStream clientData, String fileName) {
            Path destFile = root.resolve(fileName);
            try {
                Path tmpFile = Files.createTempFile("recv-tmp-", ".tmp");
                Files.copy(clientData, tmpFile, StandardCopyOption.REPLACE_EXISTING);
                Files.move(tmpFile, destFile, StandardCopyOption.REPLACE_EXISTING);
                LOG.debug("File {} received from client", fileName);
            } catch (IOException ex) {
                LOG.info("Recv file failed, name=" + fileName, ex);
            }
        }

        public void sendFile(String file) {
            try (DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
                //handle file read
                Path myFile = root.resolve(file);
                if (Files.exists(myFile)) {
                    out.writeLong(Files.size(myFile));
                    Files.copy(myFile, out);
                } else {
                    out.writeLong(-1L);
                }
            } catch (IOException e) {
                LOG.info("send file failed, name=" + file, e);
            }
        }
    }
}
