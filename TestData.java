package com.BatchStreamAnalytics;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TestData {

    public static List<Tuple3<Long, String, Double>> getTestData() {
        long base = Instant.now().toEpochMilli(); // timestamp in milliseconds
        List<Tuple3<Long, String, Double>> data = new ArrayList<Tuple3<Long, String, Double>>(10);
        data.add(Tuple3.of(base, "a", 5.0));
        data.add(Tuple3.of(base + 1000, "a", 25.0));
        data.add(Tuple3.of(base + 2000, "a", 45.0));
        data.add(Tuple3.of(base + 5000, "b", 10.0));
        data.add(Tuple3.of(base + 7500, "b", 30.0));
        data.add(Tuple3.of(base + 8500, "b", 50.0));
        data.add(Tuple3.of(base + 9000, "c", 15.0));
        data.add(Tuple3.of(base + 11000, "c", 55.0));
        data.add(Tuple3.of(base + 15000, "d", 20.0));
        data.add(Tuple3.of(base + 20000, "d", 40.0));
        data.add(Tuple3.of(base + 21000, "c", 35.0));
        data.add(Tuple3.of(base + 50000, "d", 60.0));
        return data;
    }
}
