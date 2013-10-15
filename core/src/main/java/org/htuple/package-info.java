/**
 * In MapReduce using compound map output keys and customizing which fields are partitioned, sorted and grouped can be
 * tedious, especially when doing this across multiple jobs. The goal of this library is to provide a Tuple class,
 * which can contain multiple elements, and provide along with it a ShuffleUtils class to give you a easy-to-use method
 * to tune which tuple elements should be used for partitioning, sorting and grouping.
 *
 * The two classes that you as an end-user care about are:
 *
 * <ul>
 *     <li>
 *         {@link Tuple}, which allows multiple fields to be stored and fetched.
 *     </li>
 *     <li>
 *         {@link ShuffleUtils.ConfigBuilder}, which is a builder that allows you to tune how partitioning,
 *         sorting and grouping should work for a given MapReduce job using your <code>Tuple</code> instances.
 *     </li>
 * </ul>
 */
package org.htuple;