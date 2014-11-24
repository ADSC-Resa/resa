package resa.migrate;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by ding on 14-7-31.
 */
public class RemoteOutputStream extends FilterOutputStream {

    public static RemoteOutputStream create(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        return new RemoteOutputStream(socket);
    }

    private RemoteOutputStream(Socket socket) throws IOException {
        super(socket.getOutputStream());
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }
}
