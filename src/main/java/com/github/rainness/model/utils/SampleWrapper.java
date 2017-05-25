package com.github.rainness.model.utils;

import com.github.rainness.model.Feature;
import org.apache.hadoop.io.Text;

/**
 * Created by rainness on 16-12-23.
 */
public class SampleWrapper {

    private Sample sample = new Sample();

    public Sample parse(Text value, int featureSize) {
        sample.init();
        String[] array = value.toString().split("\t");
        try {
            sample.label = Double.parseDouble(array[0]);
        } catch (Exception e) {
            sample.label = array[0].equals("true") ? 1.0 : 0.0;
        }
        String[] featureArray = array[1].split("\\s");
        for (String feature : featureArray) {
            int index = Integer.parseInt(feature);
            if (index >= featureSize) {
                continue;
            }
            sample.feature.add(index);
        }
        return sample;
    }

    public class Sample {

        private double label = 0.0;
        private Feature feature = new Feature();

        private Sample() {}

        public Sample(double label, Feature feature) {
            this.label = label;
            this.feature = feature;
        }

        public double getLabel() {
            return label;
        }

        public void setLabel(double label) {
            this.label = label;
        }

        public Feature getFeature() {
            return feature;
        }

        public void setFeature(Feature feature) {
            this.feature = feature;
        }

        public void init() {
            label = 0.0;
            feature.clear();
        }
    }
}
