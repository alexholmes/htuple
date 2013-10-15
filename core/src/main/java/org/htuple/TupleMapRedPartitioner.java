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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * A partitioner for {@link org.htuple.Tuple}'s to support secondary sorting.
 * <p/>
 * See {@link ShuffleUtils} for examples of how secondary sort can be configured.
 */
public class TupleMapRedPartitioner implements Partitioner<Tuple, Object> {

    private TuplePartitioner partitioner;

    @Override
    public void configure(JobConf job) {
        partitioner = new TuplePartitioner(job);
    }

    @Override
    public int getPartition(Tuple key, Object value, int numPartitions) {
        return partitioner.getPartition(key, numPartitions);
    }

}
