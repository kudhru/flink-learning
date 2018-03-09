package com.BatchStreamAnalytics;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TumblingWindowEmulationShare extends ProcessFunction<Tuple3<Long, String, Double>, Tuple2<String, Double>> {

    private transient ValueState<Tuple2<String, Double>> sum;
    private final long windowLength;
    private Logger LOG = LoggerFactory.getLogger(TumblingWindowEmulationShare.class);

    public TumblingWindowEmulationShare(long windowLength)
    {
        this.windowLength = windowLength;
    }

    @Override
    public void processElement(Tuple3<Long, String, Double> input, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {

        // access the state value
        Tuple2<String, Double> currentSum = sum.value();

        // add the second field of the input value
        if(currentSum == null) {
            currentSum = new Tuple2<>(input.f1, 0.0);
        }
        currentSum.f1 += input.f2;

        // update the state
        sum.update(currentSum);

        long windowStartTime = TimeWindow.getWindowStartWithOffset(ctx.timestamp(), 0, windowLength);
        long windowEndTime = windowStartTime +  windowLength;
        ctx.timerService().registerEventTimeTimer(windowEndTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Double>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        Tuple2<String, Double> currentSum = sum.value();
        out.collect(currentSum);
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<String, Double>> descriptor =
                new ValueStateDescriptor<>(
                        "TumblingWindowEmulation",
                        TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {})
                );
        sum = getRuntimeContext().getState(descriptor);
    }

    public static void main(String[] args) throws Exception {
        long windowLength = 25000;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(TestData.getTestData())
                .assignTimestampsAndWatermarks(new MyTimestampAndWatermarkGenerator())
                .keyBy(1)
                .process(new TumblingWindowEmulationShare(windowLength))
                .print();
        env.execute();
    }
}
