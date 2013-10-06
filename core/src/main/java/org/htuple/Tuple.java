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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A tuple for use as intermediary map outputs. Not designed for use beyond the scope of intermediary map
 * outputs (i.e. don't use this for general-purpose persistence).
 * <p/>
 * Null values are supported. If an element is retrieved using an index value that's out of bounds (and not a negative
 * number), then a null value is be returned.
 * <p/>
 * See {@link ShuffleUtils} for examples of how custom partitioning, sorting and grouping can be configured for
 * tuples.
 * <p/>
 * Example usage:
 * <p/>
 * <pre><code>
 * <p/>
 * // Create a 3-element tuple, where "alex" is located at index 0, "1" at index 1 and "3" at index 2
 * Tuple t = new Tuple();
 * t.add("alex").add(1).add(3);
 * <p/>
 * // extract the third element (the number "3")
 * t.getInt(2);
 * <p/>
 * </code></pre>
 * <p/>
 * You can also use enum's to get and set data for increased comprehension. The enum ordinal value
 * (@see http://docs.oracle.com/javase/7/docs/api/java/lang/Enum.html#ordinal())
 * is used as the index.
 * <p/>
 * <pre><code>
 * enum MyTupleFields { ID, NAME }
 * ...
 * Tuple t = new Tuple();
 * t.set(MyTupleFields.ID, 123);
 * t.set(MyTupleFields.NAME, "alex");
 * <p/>
 * t.getString(MyTupleFields.NAME);
 * </code></pre>
 */
public class Tuple implements WritableComparable<Tuple> {

    /**
     * The container for the elements.
     */
    private List<Object> fields = new ArrayList<Object>();

    public Tuple clear() {
        fields.clear();
        return this;
    }

    public int size() {
        return fields.size();
    }

    public Tuple set(int idx, Object val) {
        while (idx >= fields.size()) {
            fields.add(null);
        }
        fields.set(idx, val);
        return this;
    }

    public Tuple set(Enum<?> eval, Object val) {
        set(eval.ordinal(), val);
        return this;
    }

    public Tuple set(Enum<?> eval, Short val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, Short val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, Integer val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, Integer val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, Long val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, String val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, String val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, Boolean val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, Double val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, Double val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, Float val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, Float val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple set(Enum<?> eval, BytesWritable val) {
        set(eval, (Object) val);
        return this;
    }

    public Tuple set(int idx, BytesWritable val) {
        set(idx, (Object) val);
        return this;
    }

    public Tuple add(Short val) {
        fields.add(val);
        return this;
    }

    public Tuple add(Integer val) {
        fields.add(val);
        return this;
    }

    public Tuple add(Long val) {
        fields.add(val);
        return this;
    }

    public Tuple add(String val) {
        fields.add(val);
        return this;
    }

    public Tuple add(Boolean val) {
        fields.add(val);
        return this;
    }

    public Tuple add(Double val) {
        fields.add(val);
        return this;
    }

    public Tuple add(Float val) {
        fields.add(val);
        return this;
    }

    public Tuple add(BytesWritable val) {
        fields.add(val);
        return this;
    }

    public Object getObject(int idx) {
        if (idx < 0) {
            throw new IllegalArgumentException("Negative size: " + idx);
        }
        if (idx >= fields.size()) {
            return null;
        }
        return fields.get(idx);
    }

    public Object getObject(Enum<?> eval) {
        return getObject(eval.ordinal());
    }

    public Short getShort(Enum<?> eval) {
        return (Short) getObject(eval);
    }

    public Short getShort(int idx) {
        return (Short) getObject(idx);
    }

    public Integer getInt(Enum<?> eval) {
        return (Integer) getObject(eval);
    }

    public Integer getInt(int idx) {
        return (Integer) getObject(idx);
    }

    public Long getLong(Enum<?> eval) {
        return (Long) getObject(eval);
    }

    public Long getLong(int idx) {
        return (Long) getObject(idx);
    }

    public String getString(Enum<?> eval) {
        return (String) getObject(eval);
    }

    public String getString(int idx) {
        return (String) getObject(idx);
    }

    public Boolean getBoolean(Enum<?> eval) {
        return (Boolean) getObject(eval);
    }

    public Boolean getBoolean(int idx) {
        return (Boolean) getObject(idx);
    }

    public Double getDouble(Enum<?> eval) {
        return (Double) getObject(eval);
    }

    public Double getDouble(int idx) {
        return (Double) getObject(idx);
    }

    public Float getFloat(Enum<?> eval) {
        return (Float) getObject(eval);
    }

    public Float getFloat(int idx) {
        return (Float) getObject(idx);
    }

    public BytesWritable getBytes(Enum<?> eval) {
        return (BytesWritable) getObject(eval);
    }

    public BytesWritable getBytes(int idx) {
        return (BytesWritable) getObject(idx);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, fields.size());
        for (Object element : fields) {
            SerializationUtils.write(out, element);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fields.clear();
        int elementCount = WritableUtils.readVInt(in);
        while (elementCount-- > 0) {
            fields.add(SerializationUtils.read(in));
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields.toArray());
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Tuple))
            return false;

        Tuple other = (Tuple) object;

        if (this.fields.size() != other.fields.size())
            return false;

        for (int i = 0; i < this.fields.size(); i++) {
            Object lhs = this.fields.get(i);
            Object rhs = other.fields.get(i);

            if (lhs == null && rhs == null) {
                continue;
            }

            if (lhs == null || rhs == null) {
                return false;
            }

            if (!lhs.equals(rhs)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < fields.size(); i++) {
            sb.append("[").append(i).append("]='").append(fields.get(i)).append(", ");
        }
        return sb.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Tuple other) {

        for (int i = 0; i < this.fields.size() && i < other.fields.size(); i++) {

            Object lhs = this.fields.get(i);
            Object rhs = other.fields.get(i);

            if (lhs == null && rhs == null) {
                continue;
            }

            int cmp = lhs == null ? -1 : rhs == null ? 1 : 0;

            if (cmp != 0) {
                return cmp;
            }

            cmp = ((Comparable) lhs).compareTo(rhs);

            if (cmp != 0) {
                return cmp;
            }
        }

        return fields.size() - other.fields.size();
    }

}
