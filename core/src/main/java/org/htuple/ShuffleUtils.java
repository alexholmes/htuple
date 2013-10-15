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

package org.htuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Utilities to help with custom sorting, grouping and partitioning of {@link Tuple}'s.
 * <p/>
 * The following example shows how you'd configure your job for secondary sort for tuples with two elements, assuming
 * that all the elements would be used for partitioning and grouping, but only the first element would be used for
 * grouping.
 * <p/>
 * <pre><code>
 * ShuffleUtils.configBuilder()
 *   .setPartitionerIndices(0)
 *   .setSortIndices(0, 1)
 *   .setGroupIndices(0)
 *   .configure(conf);
 * <p/>
 * </code></pre>
 * <p/>
 * This class also supports using enum's to improve the readability of your code (just like with Tuples).
 * <p/>
 * <p/>
 * <pre><code>
 * enum MyTupleFields { ID, NAME }
 * ...
 * ShuffleUtils.configBuilder()
 *   .setPartitionerIndices(MyTupleFields.values())
 *   .setSortIndices(MyTupleFields.values())
 *   .setGroupIndices(MyTupleFields.ID)
 *   .configure(conf);
 * <p/>
 * </code></pre>
 */
public class ShuffleUtils {

    public static final String BASE_CONFIG_NAME = "org.htuple";
    public static final String PARTITIONER_INDEXES_CONFIG_NAME = BASE_CONFIG_NAME + ".partitioner.indexes";
    public static final String SORTING_INDEXES_CONFIG_NAME = BASE_CONFIG_NAME + ".sorting.indexes";
    public static final String GROUPING_INDEXES_CONFIG_NAME = BASE_CONFIG_NAME + ".grouping.indexes";

    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }

    /**
     * A builder that allows you to tune how partitioning,
     * sorting and grouping should work for a given MapReduce job using your {@link Tuple} instances.
     */
    public static class ConfigBuilder {
        int[] partitionerIndices;
        int[] sortIndices;
        int[] groupIndices;
        boolean useNewApi;

        /**
         * Indicate that the old {@code org.apache.hadoop.mapred} API should be used when configuring
         * the job.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder useOldApi() {
            this.useNewApi = false;
            return this;
        }

        /**
         * Indicate that the new {@code org.apache.hadoop.mapreduce} API should be used when configuring
         * the job.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder useNewApi() {
            this.useNewApi = true;
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link Partitioner}.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setPartitionerIndices(int... indices) {
            this.partitionerIndices = indices;
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link Partitioner}.
         * The ordinal values of the supplied arguments are used to determine the indexes.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setPartitionerIndices(Enum<?>... indices) {
            this.partitionerIndices = enumOrdinalsToArray(indices);
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link TupleComparator} for sorting.
         * <p/>
         * These indexes are used according to the order that they are supplied to this method. In other words,
         * if you call {@code setSortIndices(2, 1);}, then the 3rd element will be used for sorting
         * followed by the second element.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setSortIndices(int... indices) {
            this.sortIndices = indices;
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link TupleComparator} for sorting.
         * The ordinal values of the supplied arguments are used to determine the indexes.
         * <p/>
         * These indexes are used according to the order that they are supplied to this method. In other words,
         * if you call {@code setSortIndices(2, 1);}, then the 3rd element will be used for sorting
         * followed by the second element.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setSortIndices(Enum<?>... indices) {
            this.sortIndices = enumOrdinalsToArray(indices);
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link TupleComparator} for grouping.
         * <p/>
         * These indexes are used according to the order that they are supplied to this method. In other words,
         * if you call {@code setGroupIndices(2, 1);}, then the 3rd element will be used for grouping
         * followed by the second element.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setGroupIndices(int... indices) {
            this.groupIndices = indices;
            return this;
        }

        /**
         * Set the tuple indexes that will be used by the {@link TupleComparator} for grouping.
         * The ordinal values of the supplied arguments are used to determine the indexes.
         * <p/>
         * These indexes are used according to the order that they are supplied to this method. In other words,
         * if you call {@code setGroupIndices(2, 1);}, then the 3rd element will be used for grouping
         * followed by the second element.
         *
         * @return a handle to this object to enable builder operations
         */
        public ConfigBuilder setGroupIndices(Enum<?>... indices) {
            this.groupIndices = enumOrdinalsToArray(indices);
            return this;
        }

        /**
         * Configure the supplied configuration object with any partitioner, sorting and grouping
         * configs that were setup prior to calling this method.
         *
         * @param conf the Hadoop configuration to be populated
         */
        public void configure(Configuration conf) {
            if (useNewApi) {
                configureIndexes(conf, partitionerIndices, PARTITIONER_INDEXES_CONFIG_NAME, "mapreduce.partitioner.class", TupleMapReducePartitioner.class, Partitioner.class);
            } else {
                configureIndexes(conf, partitionerIndices, PARTITIONER_INDEXES_CONFIG_NAME, "mapred.partitioner.class", TupleMapRedPartitioner.class, org.apache.hadoop.mapred.Partitioner.class);
            }
            configureIndexes(conf, sortIndices, SORTING_INDEXES_CONFIG_NAME, "mapred.output.key.comparator.class", TupleSortComparator.class, RawComparator.class);
            configureIndexes(conf, groupIndices, GROUPING_INDEXES_CONFIG_NAME, "mapred.output.value.groupfn.class", TupleGroupingComparator.class, RawComparator.class);
        }

        private void configureIndexes(Configuration conf, int[] indexes, String configName, String name, Class<?> theClass, Class<?> xface) {
            if (indexes != null) {
                conf.setStrings(configName, indexesToStrings(indexes));
                conf.setClass(name, theClass, xface);
            }
        }

        public static int[] enumOrdinalsToArray(Enum<?>... enums) {
            int[] array = new int[enums.length];
            for (int i = 0; i < enums.length; i++) {
                array[i] = enums[i].ordinal();
            }
            return array;
        }
    }

    public static int[] indexesFromConfig(Configuration conf, String confName) {
        String[] parts = conf.getStrings(confName);

        if (parts == null || parts.length == 0) {
            throw new IllegalStateException(String.format("Empty or no configuration set for '%s'", confName));
        }

        int[] indices = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            indices[i] = Integer.valueOf(parts[i]);
        }
        return indices;
    }

    public static String[] indexesToStrings(int[] indexes) {
        if (indexes == null || indexes.length == 0) {
            throw new IllegalStateException("Empty or null indexes");
        }

        String[] parts = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            parts[i] = String.valueOf(indexes[i]);
        }
        return parts;
    }
}
