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

import org.apache.hadoop.io.BytesWritable;
import org.htuple.Tuple;
import org.junit.Test;
import org.omg.CORBA.OBJECT_NOT_EXIST;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

/**
 * Basic tests for the {@link Tuple} class.
 */
public class TupleTest {

    @Test
    public void testEmptyEqualsAndHash() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void testFlippedEqualsAndHash() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add(2).add(1);
        t2.add(1).add(2);

        assertNotSame(t1, t2);
        assertNotSame(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void testMultiElementEqualsAndHash() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));
        t2.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));

        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void testMultiElementNotEqualsAndHash() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));
        t2.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,1}));

        assertNotSame(t1, t2);
        assertNotSame(t1.hashCode(), t2.hashCode());
    }

    @Test
    public void testGetMultipleElements() throws Exception {
        Tuple t1 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));

        assertEquals("bob", t1.getString(0));
        assertEquals(1, (int) t1.getInt(1));
        assertEquals(2L, (long) t1.getLong(2));
        assertEquals((short) 3, (short) t1.getShort(3));
        assertTrue(t1.getBoolean(4));
        assertNull(t1.getBoolean(5));
        assertEquals(12.3, t1.getDouble(6), 0.01);
        assertEquals(21.2f, t1.getFloat(7), 0.01);
        assertEquals(new BytesWritable(new byte[]{1,2,3}), t1.getBytes(8));
    }

    enum TestEnum {
        FIRST, SECOND, THIRD
    }

    @Test
    public void testEnumGetSet() throws Exception {
        Tuple t = new Tuple();

        t.set(TestEnum.THIRD, 5);

        assertEquals(5, (int) t.getInt(2));
        assertEquals(5, (int) t.getInt(TestEnum.THIRD));
        assertNull(t.getObject(0));
        assertNull(t.getObject(1));

        t.set(TestEnum.FIRST, "first");
        t.set(TestEnum.SECOND, 3.4);

        assertEquals("first", t.getString(0));
        assertEquals("first", t.getString(TestEnum.FIRST));

        assertEquals(3.4, t.getDouble(1), 0.1);
        assertEquals(3.4, t.getDouble(TestEnum.SECOND), 0.1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWithNegativeIndex() {
        new Tuple().getDouble(-1);
    }

    public void testPositiveIndexOutOfBounds() {
        assertNull(new Tuple().getDouble(0));
    }

    @Test
    public void testSerialization() throws Exception {
        Tuple t1 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(os);

        t1.write(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

        Tuple t2 = new Tuple();

        // ensure that existing values are wiped-out by injecting some prior to deserialization

        t2.add("foo");

        t2.readFields(dis);

        assertEquals(t1, t2);

        assertEquals("bob", t2.getString(0));
        assertEquals(1, (int) t2.getInt(1));
        assertEquals(2L, (long) t2.getLong(2));
        assertEquals((short) 3, (short) t2.getShort(3));
        assertTrue(t2.getBoolean(4));
        assertNull(t2.getBoolean(5));
        assertEquals(12.3, t2.getDouble(6), 0.01);
        assertEquals(21.2f, t2.getFloat(7), 0.01);
        assertEquals(new BytesWritable(new byte[]{1,2,3}), t2.getBytes(8));
    }

    @Test
    public void testClear() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));

        assertNotSame(t1, t2);

        t1.clear();

        assertEquals(t1, t2);
    }

    @Test
    public void testCompareTo() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));
        t2.add("bob").add(1).add(2L).add((short) 3).add(true).add((String) null).add(12.3).add(21.2f).add(new BytesWritable(new byte[]{1,2,3}));

        assertEquals(0, t1.compareTo(t2));
    }

    @Test
    public void testEqualCompareTo() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex");
        t2.add("alex");
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add((short) 1);
        t2.clear().add((short) 1);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(1);
        t2.clear().add(1);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(1L);
        t2.clear().add(1L);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(1.0);
        t2.clear().add(1.0);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(1.0f);
        t2.clear().add(1.0f);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(false);
        t2.clear().add(false);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(true);
        t2.clear().add(true);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(new BytesWritable(new byte[]{1,2,3}));
        t2.clear().add(new BytesWritable(new byte[]{1,2,3}));
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add((Integer) null);
        t2.clear().add((Integer) null);
        assertEquals(0, t1.compareTo(t2));

        t1.clear().add(1).add(2);
        t2.clear().add(1).add(2);
        assertEquals(0, t1.compareTo(t2));
    }

    @Test
    public void testLhsLessThanCompareTo() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex");
        t2.add("bob");
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add((short) 1);
        t2.clear().add((short) 2);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(1);
        t2.clear().add(2);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(1L);
        t2.clear().add(2L);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(1.0);
        t2.clear().add(2.0);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(1.0f);
        t2.clear().add(2.0f);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(false);
        t2.clear().add(true);
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(new BytesWritable(new byte[]{1,2,3}));
        t2.clear().add(new BytesWritable(new byte[]{2}));
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add((Integer) null);
        t2.clear().add(new BytesWritable(new byte[]{2}));
        assertTrue(t1.compareTo(t2) < 0);

        t1.clear().add(1);
        t2.clear().add(1).add(2);
        assertTrue(t1.compareTo(t2) < 0);
    }

    @Test
    public void testLhsGreaterThanCompareTo() throws Exception {
        Tuple t1 = new Tuple();
        Tuple t2 = new Tuple();

        t1.add("alex");
        t2.add("bob");
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add((short) 1);
        t2.clear().add((short) 2);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(1);
        t2.clear().add(2);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(1L);
        t2.clear().add(2L);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(1.0);
        t2.clear().add(2.0);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(1.0f);
        t2.clear().add(2.0f);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(false);
        t2.clear().add(true);
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(new BytesWritable(new byte[]{1,2,3}));
        t2.clear().add(new BytesWritable(new byte[]{2}));
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add((Integer) null);
        t2.clear().add(new BytesWritable(new byte[]{2}));
        assertTrue(t2.compareTo(t1) > 0);

        t1.clear().add(1);
        t2.clear().add(1).add(2);
        assertTrue(t2.compareTo(t1) > 0);
    }
}

