package resa.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import resa.optimize.AllocResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by ding on 14/12/4.
 */
public class JsonTest {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void writeString() throws IOException {
        System.out.println(mapper.writeValueAsString("abc"));
    }

    @Test
    public void writeList() throws IOException {
        System.out.println(mapper.writeValueAsString(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void writeAllocResult() throws IOException {
        System.out.println(mapper.writeValueAsString(new AllocResult(AllocResult.Status.FEASIBLE,
                Collections.singletonMap("bolt", 1), Collections.singletonMap("bolt", 2))
                .setContext(Collections.singletonMap("lambda", 1))));
    }

}
