package com.github.rainness.model.merge;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rainness on 16-12-26.
 */
public class CombineShuffleValueWritable implements Writable {

    private IntWritable intWritable = new IntWritable();
    private DoubleWritable doubleWritable = new DoubleWritable();

    public IntWritable getIntWritable() {
        return intWritable;
    }

    public void setIntWritable(int intWritable) {
        this.intWritable.set(intWritable);
    }

    public DoubleWritable getDoubleWritable() {
        return doubleWritable;
    }

    public void setDoubleWritable(double doubleWritable) {
        this.doubleWritable.set(doubleWritable);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        intWritable.write(dataOutput);
        doubleWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        intWritable.readFields(dataInput);
        doubleWritable.readFields(dataInput);
    }
}
