package org.dbanelas;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CircularBuffer<T> implements Serializable {
    private static final long serialVersionUID = 3L;

    private final int K;
    private final T[] buffer;
    private int head;
    private int size;

    public CircularBuffer(int K) {
        if (K <= 0) throw new IllegalArgumentException("K must be > 0");
        this.K = K;
        this.buffer = (T[]) new Object[K];
        this.head = 0;
        this.size = 0;
    }

    public void add(T item) {
        int tail = (head + size) % K;
        buffer[tail] = item;
        if (size < K) size++;
        else head = (head + 1) % K;
    }

    public T get(int index) {
        Objects.checkIndex(index, size);
        return buffer[(head + index) % K];
    }


    public List<T> range(int from, int to) {
        Objects.checkFromToIndex(from, to, size);
        int length = to - from;
        List<T> result = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
           result.add(get(from + i));
        }
        return result;
    }

    public int size() {
        return size;
    }

    public void clear() {
        head = 0;
        size = 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CircularBuffer{");
        for (int i = 0; i < size; i++) {
            sb.append(buffer[(head + i) % K]);
            if (i < size - 1) sb.append(", ");
        }
        sb.append("}");
        return sb.toString();
    }
}
