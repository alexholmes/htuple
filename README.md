htuple
======

## Description

In MapReduce using compound map output keys and customizing which fields are partitioned, sorted and grouped can be
tedious, especially when doing this across multiple jobs. The goal of this library is to provide a `Tuple` class,
which can contain multiple elements, and provide along with it a `ShuffleUtils` class to give you a easy-to-use
method to tune which tuple elements should be used for partitioning, sorting and grouping.

## License

Apache version 2.0. For more details look at [LICENSE](LICENSE).

## Example

Imagine that you are working with people names in MapReduce. Your mapper emits <last-name, first-name> records, and in your
reducer you want the first names to be streamed in sorted order. This is what is known as secondary sort.

If you were to use htuple to implement secondary sort, the first thing I would recommend would be to create an
enum to represent the fields in the Tuple to help with the readability of your code.

    /**
     * User-friendly names that we can use to refer to fields in the tuple.
     */
    enum TupleFields {
        LAST_NAME,
        FIRST_NAME
    }

Next in your MapReduce driver code you'd use the `ShuffleUtils` to configure how the partitioner, sorter and grouper
behave. Since our example is secondary sort, we want both the partitioner and grouper to use only the last name, but
the sorter should use both the last and first name.

    ShuffleUtils.configBuilder()
            .useNewApi()
            .setPartitionerIndices(TupleFields.LAST_NAME)
            .setSortIndices(TupleFields.values())
            .setGroupIndices(TupleFields.LAST_NAME)
            .configure(conf);

A couple of things worth noting in the above example:

1.  We're using the new MapReduce API (i.e. using package `org.apache.hadoop.mapreduce`), and as such you need to call the `useNewApi` method.
2.  The `values` method on enums emits all of the enum elements in order of definition, which in our example is the last
name followed by the first name - exactly the order in which we want the sorting to occur.

Now all that's left is to use the `Tuple` class in the mapper. Below we assume that each line in the input file is
in the form `last_name <TAB> first_name`.

    public static class Map extends Mapper<LongWritable, Text, Tuple, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // tokenize the line
            String nameParts[] = value.toString().split("\t");

            // create the tuple, setting the first and last names
            Tuple outputKey = new Tuple();
            outputKey.set(TupleFields.LAST_NAME, nameParts[0]);
            outputKey.set(TupleFields.FIRST_NAME, nameParts[1]);

            // emit the tuple and the original contents of the line
            context.write(outputKey, value);
        }
    }

To read the entire source of this example please take a look at [SecondarySort.java](https://github.com/alexholmes/htuple/blob/master/examples/src/main/java/org/htuple/examples/SecondarySort.java).
You can also execute this example by following these steps, which assume you've already built the project
(see "Building" if you haven't yet done a build):

    $ ./run-example.sh input output

This script will run the `SecondarySort` class, which writes some sample input in HDFS in the `input` directory (the first
argument supplied to the `run-example.sh` script), and runs a secondary sort MapReduce job, where the output is written
to the `output` directory, the second argument supplied to the script.

After the job completes the output directory will contain the results of the job, which will show the last names and
first names in sorted order:

    $ hadoop fs -cat output/part*
    Smith	Anne
    Smith	John
    Smith	Ken

## Building

To build the core and example JAR's, first clone the repo:

    $ git clone https://github.com/alexholmes/htuple

Then simply go into the `htuple` directory and run Maven:

    $ mvn package

After this is complete you'll have both core and example JAR's:

    $ ls core/target/* examples/target/*

## Documentation

The JavaDoc's for the project can be extracted from the JavaDoc JAR's that are generated under the `core/target` directory after building the project.

## Additional Resources

* Learn about the datatypes supported: [DATATYPES.md](DATATYPES.md)

