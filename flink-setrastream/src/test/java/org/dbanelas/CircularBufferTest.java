package org.dbanelas;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CircularBufferTest {

    @Test
    public void testConstructorWithValidSize() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(5);
        assertEquals(0, buffer.size());
    }

    @Test
    public void testConstructorWithInvalidSize() {
        assertThrows(IllegalArgumentException.class, () -> new CircularBuffer<>(0));
        assertThrows(IllegalArgumentException.class, () -> new CircularBuffer<>(-1));
    }

    @Test
    public void testAddAndGet() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        assertEquals(3, buffer.size());
        assertEquals(Integer.valueOf(1), buffer.get(0));
        assertEquals(Integer.valueOf(2), buffer.get(1));
        assertEquals(Integer.valueOf(3), buffer.get(2));
    }

    @Test
    public void testAddOverwritesOldest() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        buffer.add(4);
        assertEquals(3, buffer.size());
        assertEquals(Integer.valueOf(2), buffer.get(0));
        assertEquals(Integer.valueOf(3), buffer.get(1));
        assertEquals(Integer.valueOf(4), buffer.get(2));
    }

    @Test
    public void testGetWithInvalidIndex() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);
        buffer.add(1);
        buffer.add(2);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(2));
    }

    @Test
    public void testRange() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(5);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        buffer.add(4);
        List<Integer> subRange = buffer.range(1, 3);
        assertEquals(List.of(2, 3), subRange);
    }

    @Test
    public void testRangeWithInvalidIndices() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(5);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.range(-1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.range(1, 4));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.range(2, 1));
    }

    @Test
    public void testClear() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        buffer.clear();
        assertEquals(0, buffer.size());
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(0));
    }

    @Test
    public void testToString() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        assertEquals("CircularBuffer{1, 2, 3}", buffer.toString());
        buffer.add(4);
        assertEquals("CircularBuffer{2, 3, 4}", buffer.toString());
    }
}
