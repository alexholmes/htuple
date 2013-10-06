/*
 * Copyright 2013 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.htuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.htuple.Tuple;
import org.htuple.examples.SecondarySort;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Basic tests for the {@link org.htuple.examples.SecondarySort} class.
 */
public class SecondarySortTest {

    MapReduceDriver<LongWritable, Text, Tuple, Text, Text, NullWritable> mapReduceDriver;

    @Before
    public void setUp() {
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(new SecondarySort.Map(), new SecondarySort.Reduce());
    }

    @Test
    public void testSecondarySort() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();

        SecondarySort.setupSecondarySort(conf);

        LongWritable dummyMapKey = new LongWritable(1);

        mapReduceDriver
                .withInput(dummyMapKey, new Text("Smith\tJohn\n"))
                .withInput(dummyMapKey, new Text("Smith\tAnne\n"))
                .withInput(dummyMapKey, new Text("Smith\tKen\n"))
                .withOutput(new Text("Smith\tAnne\n"), NullWritable.get())
                .withOutput(new Text("Smith\tJohn\n"), NullWritable.get())
                .withOutput(new Text("Smith\tKen\n"), NullWritable.get())
                .runTest(true);
    }}

