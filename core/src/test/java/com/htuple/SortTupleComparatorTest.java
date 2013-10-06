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
import org.htuple.TupleComparator;

import org.htuple.TupleSortComparator;

/**
 * Tests for {@link org.htuple.TupleSortComparator} class.
 */
public class SortTupleComparatorTest extends AbstractTupleComparatorTest {

    @Override
    public TupleComparator getComparator() {
        return new TupleSortComparator();
    }

    @Override
    public String getConfigName() {
        return ShuffleUtils.SORTING_INDEXES_CONFIG_NAME;
    }

    @Override
    public void setComparatorIndices(Configuration conf, int... indices) {
        ShuffleUtils.configBuilder().setSortIndices(indices).configure(conf);
    }
}

