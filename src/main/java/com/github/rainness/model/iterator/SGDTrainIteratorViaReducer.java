package com.github.rainness.model.iterator;

import com.github.rainness.model.utils.LocalContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rainness on 16-12-23.
 */
public class SGDTrainIteratorViaReducer extends Reducer<IteratorShuffleKeyWritable, IteratorShuffleValueWritable, IntWritable, DoubleWritable> {

    private static Logger LOG = Logger.getLogger(SGDTrainIteratorViaReducer.class);
    private LocalContext localContext = new LocalContext();
    private Map<Integer, Double> gradientMap = new HashMap();
    private double[] coefficient;
    private int sampleNumber = 0;
    private double learningRate;
    private IntWritable intWritable = new IntWritable();
    private DoubleWritable doubleWritable = new DoubleWritable();

    @Override
    public void setup(Context context) {
        localContext.parse(context, Context.class);
        coefficient = localContext.getCoefficient();
        learningRate = localContext.getLearningRate();
    }

    @Override
    public void reduce(IteratorShuffleKeyWritable key, Iterable<IteratorShuffleValueWritable> values, Context context) {
        try {
            sampleNumber = 0;
            for (IteratorShuffleValueWritable value : values) {
                update(gradientMap, value.getDelta(), value.getFeature());
                sampleNumber++;
            }
            //update coefficient
            for (Integer i : gradientMap.keySet()) {
                coefficient[i] = coefficient[i] + learningRate * 1.0 / sampleNumber * gradientMap.get(i);
            }
            for (int i = 0; i < coefficient.length; i++) {
                intWritable.set(i);
                doubleWritable.set(coefficient[i]);
                context.write(intWritable, doubleWritable);
            }
        } catch (Exception e) {
            LOG.error("Class SGDTrainViaReducer function[reduce] error:" + e);
        }
    }

    private void update(Map<Integer, Double> gradientMap, double delta, String feature) {
        String[] value = feature.split(",");
        for (String i : value) {
            int index = Integer.parseInt(i);
            if (gradientMap.containsKey(index)) {
                double gradient = gradientMap.get(index) + delta;
                gradientMap.put(index, gradient);
            } else {
                gradientMap.put(index, delta);
            }
        }
    }
}
