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

package org.htuple.examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * An example MapReduce job showing how the {@link Tuple} and {@link ShuffleUtils} can be used in conjunction with
 * each other to secondary sort people's names.
 */
public final class SecondarySort extends Configured implements Tool {

    /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SecondarySort(), args);
        System.exit(res);
    }

    /**
     * User-friendly names that we can use to refer to fields in the tuple.
     */
    enum TupleFields {
        LAST_NAME,
        FIRST_NAME
    }

    /**
     * Sample input used by this example job.
     */
    public static final String[] EXAMPLE_NAMES = new String[]{"Smith\tJohn\n", "Smith\tAnne\n", "Smith\tKen\n"};

    /**
     * Writes the contents of {@link #EXAMPLE_NAMES} into a file in the job input directory in HDFS.
     *
     * @param conf     the Hadoop config
     * @param inputDir the HDFS input directory where we'll write a file
     * @throws IOException if something goes wrong
     */
    public static void writeInput(Configuration conf, Path inputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(inputDir)) {
            throw new IOException(String.format("Input directory '%s' exists - please remove and rerun this example", inputDir));
        }

        OutputStreamWriter writer = new OutputStreamWriter(fs.create(new Path(inputDir, "input.txt")));
        for (String name : EXAMPLE_NAMES) {
            writer.write(name);
        }
        IOUtils.closeStream(writer);
    }

    /**
     * The MapReduce driver - setup and launch the job.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Configuration conf = super.getConf();

        writeInput(conf, new Path(input));

        setupSecondarySort(conf);

        Job job = new Job(conf);
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    /**
     * Partition and group on just the last name; sort on both last and first name.
     *
     * @param conf the Hadoop config
     */
    public static void setupSecondarySort(Configuration conf) {
        ShuffleUtils.configBuilder()
                .useNewApi()
                .setPartitionerIndices(TupleFields.LAST_NAME)
                .setSortIndices(TupleFields.values())
                .setGroupIndices(TupleFields.LAST_NAME)
                .configure(conf);
    }

    /**
     * Split the input line and return a {@link Tuple} representation of the last and first names.
     *
     * @param line a line containing a tab-delimited last and first name.
     * @return a {@link Tuple} representation of the line
     */
    public static Tuple stringToTuple(String line) {
        // tokenize the line
        String nameParts[] = line.split("\t");

        // create the tuple, setting the first and last names
        Tuple tuple = new Tuple();
        tuple.set(TupleFields.LAST_NAME, nameParts[0]);
        tuple.set(TupleFields.FIRST_NAME, nameParts[1]);
        return tuple;
    }

    /**
     * This map class simply tokenizes each input line, and emits a (tuple, line) pair,
     * where the tuple contains the last and first name.
     */
    public static class Map extends Mapper<LongWritable, Text, Tuple, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Tuple outputKey = stringToTuple(value.toString());

            // emit the tuple and the original contents of the line
            context.write(outputKey, value);
        }
    }

    /**
     * The reducer just emits the map output values, allowing us to examine the resulting output and
     * determine the results of the secondary sort.
     */
    public static class Reduce extends Reducer<Tuple, Text, Text, NullWritable> {

        @Override
        public void reduce(Tuple key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }
}
