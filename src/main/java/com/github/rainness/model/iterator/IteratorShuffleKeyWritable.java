package com.github.rainness.model.iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by rainness on 16-12-26.
 */
public class IteratorShuffleKeyWritable extends IntWritable {

    public static class ShufflePartitioner extends Partitioner<IntWritable, IteratorShuffleValueWritable> {

        @Override
        public int getPartition(IntWritable intWritable, IteratorShuffleValueWritable valueWritable, int numPartitions) {
            return intWritable.get() % numPartitions;
        }
    }

    public static class ShuffleGroupingComparator implements RawComparator<IntWritable> {

        private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return INT_COMPARATOR.compare(b1, s1, 4, b2, s2, 4);
        }

        @Override
        public int compare(IntWritable o1, IntWritable o2) {
            return Integer.compare(o1.get(), o2.get());
        }
    }
}
