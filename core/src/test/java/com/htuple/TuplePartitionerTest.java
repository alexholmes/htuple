/*
 * Copyright 2013 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.htuple;

import org.apache.hadoop.conf.Configuration;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;
import org.htuple.TuplePartitioner;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Basic tests for the {@link org.htuple.TuplePartitioner} class.
 */
public class TuplePartitionerTest {

    Configuration conf;

    @Before
    public void setup() {
        conf = new Configuration();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoConfigSet() {
        new TuplePartitioner(new Configuration());
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyConfig() {
        conf.setStrings(ShuffleUtils.PARTITIONER_INDEXES_CONFIG_NAME, "");
        new TuplePartitioner(conf);
    }

    @Test
    public void testDifferentPartitions() {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex").add(1).add(3);
        t2.add("alex").add(2).add(2);

        ShuffleUtils.configBuilder().setPartitionerIndices(0).configure(conf);
        TuplePartitioner partitioner = new TuplePartitioner(conf);
        assertEquals(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));

        ShuffleUtils.configBuilder().setPartitionerIndices(1).configure(conf);
        partitioner = new TuplePartitioner(conf);
        assertNotSame(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));

        ShuffleUtils.configBuilder().setPartitionerIndices(2).configure(conf);
        partitioner = new TuplePartitioner(conf);
        assertNotSame(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));

        ShuffleUtils.configBuilder().setPartitionerIndices(0, 1).configure(conf);
        partitioner = new TuplePartitioner(conf);
        assertNotSame(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));

        ShuffleUtils.configBuilder().setPartitionerIndices(0, 2).configure(conf);
        partitioner = new TuplePartitioner(conf);
        assertNotSame(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));
    }

    @Test
    public void testIndexOutOfBounds() {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex").add(1).add(3);
        t2.add("alex").add(2).add(2);

        ShuffleUtils.configBuilder().setPartitionerIndices(0, 5).configure(conf);
        TuplePartitioner partitioner = new TuplePartitioner(conf);
        assertEquals(partitioner.getPartition(t1, 10), partitioner.getPartition(t2, 10));
    }
}

