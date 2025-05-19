package org.dbanelas;

import org.github.jamm.MemoryMeter;   // with JAMM
public final class Mem {
    static final MemoryMeter meter = MemoryMeter.builder().build();
    public static long deepSize(Object o) { return meter.measureDeep(o); }
}
