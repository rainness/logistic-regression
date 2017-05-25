package com.github.rainness.model.iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rainness on 16-12-23.
 */
public class IteratorShuffleValueWritable implements Writable {

    private DoubleWritable delta = new DoubleWritable();
    private Text feature = new Text();

    public void setDelta(double delta) {
        this.delta.set(delta);
    }

    public double getDelta() {
        return delta.get();
    }

    public void setFeature(String feature) {
        this.feature.set(feature);
    }

    public String getFeature() {
        return feature.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        delta.write(dataOutput);
        feature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        delta.readFields(dataInput);
        feature.readFields(dataInput);
    }
}
