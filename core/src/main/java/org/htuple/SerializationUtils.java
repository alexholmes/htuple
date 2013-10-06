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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 */
public class SerializationUtils {
    protected interface TupleElementSerializer {
        void write(DataOutput stream, Object element) throws IOException;

        Object read(DataInput stream) throws IOException;

        int getElementTypeId();
    }

    public final static int NULL_ELEMENT_TYPE = 9;

    private static final Map<Class, TupleElementSerializer> staticTupleElementWriters = new IdentityHashMap<Class, TupleElementSerializer>();
    private static final Map<Integer, TupleElementSerializer> staticElementIdWriters = new HashMap<Integer, TupleElementSerializer>();

    private static void populateSerializationDetails(Class clazz, TupleElementSerializer serializer) {
        staticTupleElementWriters.put(clazz, serializer);
        staticElementIdWriters.put(serializer.getElementTypeId(), serializer);
    }

    static {
        populateSerializationDetails(String.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                WritableUtils.writeString(stream, (String) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return WritableUtils.readString(stream);
            }

            @Override
            public int getElementTypeId() {
                return 1;
            }
        });

        populateSerializationDetails(Float.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                stream.writeFloat((Float) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return stream.readFloat();
            }

            @Override
            public int getElementTypeId() {
                return 2;
            }
        });

        populateSerializationDetails(Double.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                stream.writeDouble((Double) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return stream.readDouble();
            }

            @Override
            public int getElementTypeId() {
                return 3;
            }
        });

        populateSerializationDetails(Integer.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                WritableUtils.writeVInt(stream, (Integer) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return WritableUtils.readVInt(stream);
            }

            @Override
            public int getElementTypeId() {
                return 4;
            }
        });

        populateSerializationDetails(Long.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                WritableUtils.writeVLong(stream, (Long) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return WritableUtils.readVLong(stream);
            }

            @Override
            public int getElementTypeId() {
                return 5;
            }
        });

        populateSerializationDetails(Boolean.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                stream.writeBoolean((Boolean) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return stream.readBoolean();
            }

            @Override
            public int getElementTypeId() {
                return 6;
            }
        });

        populateSerializationDetails(Short.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                stream.writeShort((Short) element);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return stream.readShort();
            }

            @Override
            public int getElementTypeId() {
                return 7;
            }
        });

        populateSerializationDetails(BytesWritable.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
                ((BytesWritable) element).write(stream);
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                BytesWritable writable = new BytesWritable();
                writable.readFields(stream);
                return writable;
            }

            @Override
            public int getElementTypeId() {
                return 8;
            }
        });

        populateSerializationDetails(NullWritable.class, new TupleElementSerializer() {
            @Override
            public void write(DataOutput stream, Object element) throws IOException {
                WritableUtils.writeVInt(stream, getElementTypeId());
            }

            @Override
            public Object read(DataInput stream) throws IOException {
                return null;
            }

            @Override
            public int getElementTypeId() {
                return NULL_ELEMENT_TYPE;
            }
        });
    }

    public static void write(DataOutput stream, Object element) throws IOException {

        Class elementClass;

        if (element == null) {
            elementClass = NullWritable.class;
        } else {
            elementClass = element.getClass();
        }

        TupleElementSerializer serializer = staticTupleElementWriters.get(elementClass);
        if (serializer == null) {
            throw new IllegalArgumentException("Unsupported type: " + elementClass.getName());
        }

        serializer.write(stream, element);
    }

    public static Object read(DataInput stream) throws IOException {

        int type = WritableUtils.readVInt(stream);

        TupleElementSerializer serializer = staticElementIdWriters.get(type);
        if (serializer == null) {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }

        return serializer.read(stream);
    }
}
