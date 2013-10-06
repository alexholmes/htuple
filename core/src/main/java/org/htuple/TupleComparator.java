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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A {@link java.util.Comparator} for {@link org.htuple.Tuple} instances, which can compare a subset of the elements in
 * the tuple, which is useful for sorting and grouping when secondary-sort behavior is desired.
 * <p/>
 * TODO - Optimize space+time by overriding the {@link #compare(byte[], int, int, byte[], int, int)}.
 */
public abstract class TupleComparator extends WritableComparator implements Configurable {
    private Configuration conf;
    private int[] indices;

    public TupleComparator() {
        super(Tuple.class, true);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.indices = ShuffleUtils.indexesFromConfig(conf, getIndexConfigName());
    }

    public abstract String getIndexConfigName();

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(WritableComparable o1, WritableComparable o2) {
        Tuple lhs = (Tuple) o1;
        Tuple rhs = (Tuple) o2;
        int indexOffset = 0;
        for (int i = 0; i < lhs.size() && i < rhs.size(); i++) {
            if (i == indices[indexOffset]) {
                Object lhsObject = lhs.getObject(i);
                Object rhsObject = rhs.getObject(i);

                if (lhsObject == null && rhsObject == null) {
                    continue;
                }

                int cmp = lhsObject == null ? -1 : rhsObject == null ? 1 : 0;

                if (cmp != 0) {
                    return cmp;
                }

                cmp = ((Comparable) lhsObject).compareTo(rhsObject);

                if (cmp != 0) {
                    return cmp;
                }

                indexOffset++;

                if (indexOffset == indices.length) {
                    return 0;
                }
            }
        }
        return lhs.size() - rhs.size();
    }
}
