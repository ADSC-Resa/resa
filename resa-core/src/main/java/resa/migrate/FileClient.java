package resa.migrate;

import java.io.*;
import java.net.Socket;

/**
 * Created by ding on 14-7-31.
 */
public class FileClient {
    private static Socket sock;
    private static String fileName;
    private static BufferedReader stdin;
    private static DataOutputStream os;

    public static void main(String[] args) throws IOException, IOException {
        try {
            sock = new Socket("localhost", Integer.parseInt(args[0]));
            stdin = new BufferedReader(new InputStreamReader(System.in));
        } catch (Exception e) {
            System.err.println("Cannot connect to the server, try again later.");
            System.exit(1);
        }

        os = new DataOutputStream(sock.getOutputStream());

        try {
            switch (Integer.parseInt(selectAction())) {
                case 1:
                    os.writeUTF("write");
                    sendFile();
                    break;
                case 2:
                    os.writeUTF("read");
                    System.err.print("Enter file name: ");
                    fileName = stdin.readLine();
                    os.writeUTF(fileName);
                    receiveFile(fileName);
                    break;
            }
        } catch (Exception e) {
            System.err.println("not valid input");
        }
        sock.close();
    }

    private static String selectAction() throws IOException {
        System.out.println("1. Send file.");
        System.out.println("2. Recieve file.");
        System.out.print("\nMake selection: ");

        return stdin.readLine();
    }

    private static void sendFile() {
        try {
            System.err.print("Enter file name: ");
            fileName = stdin.readLine();

            File myFile = new File(fileName);
            byte[] mybytearray = new byte[(int) myFile.length()];

            FileInputStream fis = new FileInputStream(myFile);
            BufferedInputStream bis = new BufferedInputStream(fis);
            //bis.read(mybytearray, 0, mybytearray.length);

            DataInputStream dis = new DataInputStream(bis);
            dis.readFully(mybytearray, 0, mybytearray.length);

            OutputStream os = sock.getOutputStream();

            //Sending file name and file size to the server
            DataOutputStream dos = new DataOutputStream(os);
            dos.writeUTF(myFile.getName());
            dos.write(mybytearray, 0, mybytearray.length);
            dos.flush();
            System.out.println("File " + fileName + " sent to Server.");
        } catch (Exception e) {
            System.err.println("File does not exist!");
        }
    }

    private static void receiveFile(String fileName) {
        try {
            int bytesRead;
            InputStream in = sock.getInputStream();

            DataInputStream clientData = new DataInputStream(in);

            fileName = clientData.readUTF();
            OutputStream output = new FileOutputStream(("received_from_server_" + fileName));
            long size = clientData.readLong();
            byte[] buffer = new byte[1024];
            while (size > 0 && (bytesRead = clientData.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) {
                output.write(buffer, 0, bytesRead);
                size -= bytesRead;
            }

            output.close();
            in.close();

            System.out.println("File " + fileName + " received from Server.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static InputStream openInputStream(String host, int port, String fileName) throws IOException {
        Socket socket = new Socket(host, port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("read");
        out.writeUTF(fileName);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        if (in.readLong() < 0) {
            throw new FileNotFoundException(fileName);
        }
        return in;
    }
}
