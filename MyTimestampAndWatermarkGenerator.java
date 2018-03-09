package com.BatchStreamAnalytics;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class MyTimestampAndWatermarkGenerator implements AssignerWithPunctuatedWatermarks<Tuple3<Long, String, Double>> {
    private long currentMaxTimestamp=0;

    MyTimestampAndWatermarkGenerator(){}

    @Override
    public long extractTimestamp(Tuple3<Long, String, Double> element, long previousElementTimestamp) {
        long eventTimestamp = element.f0;
        currentMaxTimestamp = Math.max(eventTimestamp,currentMaxTimestamp);
        return eventTimestamp;
    }

    @Override
    public Watermark checkAndGetNextWatermark(Tuple3<Long, String, Double> lastElement, long extractedTimestamp)  {
        return new Watermark(extractedTimestamp);
    }
}