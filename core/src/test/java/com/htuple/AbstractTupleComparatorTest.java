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
import org.htuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import org.htuple.TupleComparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests for the {@link org.htuple.TuplePartitioner} class.
 */
public abstract class AbstractTupleComparatorTest {

    TupleComparator comparator;
    String configName;
    Configuration conf;

    public abstract TupleComparator getComparator();
    public abstract String getConfigName();
    public abstract void setComparatorIndices(Configuration conf, int ... indices);

    @Before
    public void setup() {
        comparator = getComparator();
        configName = getConfigName();
        conf = new Configuration();
    }


    @Test(expected = IllegalStateException.class)
    public void testNoConfigSetOnSort() {
        comparator.setConf(new Configuration());
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyConfigOnSort() {
        conf.setStrings(configName, "");
        comparator.setConf(conf);
    }

    @Test
    public void testDifferentSizes() {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex");
        t2.add("alex").add(1);

        setComparatorIndices(conf, 0);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) == 0);
    }

    @Test
    public void testMatchMulti() {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex").add(1).add(3);
        t2.add("alex").add(2).add(2);

        setComparatorIndices(conf, 0);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) == 0);

        setComparatorIndices(conf, 1);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) < 0);

        setComparatorIndices(conf, 2);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) > 0);

        setComparatorIndices(conf, 0, 1);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) < 0);

        setComparatorIndices(conf, 0, 2);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) > 0);
    }

    @Test
    public void testOrdering() {

        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex").add(1).add(3);
        t2.add("alex").add(2).add(2);

        setComparatorIndices(conf, 2, 1);
        comparator.setConf(conf);

        assertTrue(comparator.compare(t1, t2) > 0);
    }
}

