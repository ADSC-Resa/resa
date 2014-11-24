package resa.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * Iterate on a given iterator to get a fixed number of element
 * <p/>
 *
 * @author Troy Ding
 */
public class SizeBoundedIterable<T> implements Iterable<T> {

    private Iterable<T> input;
    private int maxSize;

    public SizeBoundedIterable(int maxSize, Iterable<T> input) {
        this.input = Objects.requireNonNull(input);
        this.maxSize = maxSize;
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(input.spliterator(), false).limit(maxSize).iterator();
    }

}
