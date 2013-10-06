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

/**
 * A partitioner for {@link Tuple}'s to support secondary sorting.
 * <p/>
 * See {@link ShuffleUtils} for examples of how secondary sort can be configured.
 */
public class TuplePartitioner {

    private final Configuration conf;
    private final int[] indices;

    public TuplePartitioner(Configuration conf) {
        this.conf = conf;
        this.indices = ShuffleUtils.indexesFromConfig(conf, ShuffleUtils.PARTITIONER_INDEXES_CONFIG_NAME);
    }

    public Configuration getConf() {
        return conf;
    }

    public int getPartition(Tuple key, int numPartitions) {

        int result = 1;
        int indexOffset = 0;
        for (int i = 0; i < key.size(); i++) {
            if (i == indices[indexOffset]) {
                Object element = key.getObject(i);
                result = 31 * result + (element == null ? 0 : element.hashCode());
                indexOffset++;

                if (indexOffset == indices.length) {
                    break;
                }
            }
        }

        return ((result * 127) & Integer.MAX_VALUE) % numPartitions;
    }
}
