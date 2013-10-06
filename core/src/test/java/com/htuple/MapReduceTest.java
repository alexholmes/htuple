package com.htuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.htuple.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Test custom partitioning, sorting and grouping.
 */
public class MapReduceTest {

    MapReduceDriver<Tuple, Tuple, Tuple, Tuple, Tuple, Tuple> mapReduceDriver;

    @Before
    public void setUp() {
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(new Mapper<Tuple, Tuple, Tuple, Tuple>(), new CountingReducer());
    }

    public static class CountingReducer extends Reducer<Tuple, Tuple, Tuple, Tuple> {
        @Override
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Tuple value: values) {
                context.write(key, value.add(count++));
            }
        }
    }

    @Test
    public void testSorting() throws IOException {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex").add(1).add(3L).add((short) 3);
        t2.add("bob").add(1).add(2L).add((short) 3);

        Configuration conf = mapReduceDriver.getConfiguration();

        ShuffleUtils.configBuilder()
                .setSortIndices(2)
                .configure(conf);

        mapReduceDriver
                .withInput(t1, t1)
                .withInput(t2, t2)
                .withOutput(t2, WritableUtils.clone(t2, conf).add(0))
                .withOutput(t1, WritableUtils.clone(t1, conf).add(0))
                .runTest(true);
    }

    @Test
    public void testGrouping() throws IOException {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();
        Tuple t3 = new Tuple();

        t1.add("alex").add(1).add(2L).add((short) 3);
        t2.add("alex").add(2).add(2L).add((short) 3);
        t3.add("bob").add(2).add(2L).add((short) 3);

        Configuration conf = mapReduceDriver.getConfiguration();
        ShuffleUtils.configBuilder()
                .setSortIndices(0, 1, 2, 3)
                .setGroupIndices(0)
                .configure(conf);

        mapReduceDriver
                .withInput(t2, t2)
                .withInput(t1, t1)
                .withInput(t3, t3)
                .withOutput(t1, WritableUtils.clone(t1, conf).add(0))
                .withOutput(t1, WritableUtils.clone(t2, conf).add(1))
                .withOutput(t3, WritableUtils.clone(t3, conf).add(0))
                .runTest(true);
    }
}
