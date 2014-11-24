package resa.migrate;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileClientTest {

    @Test
    public void testOpenInputStream() throws Exception {
        Path file = Paths.get("/tmp/abc.txt");
        Files.copy(FileClient.openInputStream("localhost", 19888, "a/simulate.yaml"), file);
    }
}