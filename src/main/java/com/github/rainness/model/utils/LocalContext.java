package com.github.rainness.model.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * Created by rainness on 16-12-26.
 */
public class LocalContext {

    private static Logger LOG = Logger.getLogger(LocalContext.class);
    private double[] coefficient;
    private String lossFunction;
    private int reduceNumber;
    private double learningRate;
    private int featureSize;
    private double samplingRate;

    public <T> void parse(T obj, Class<T> clazz) {
        Configuration conf;
        try {
            if (clazz == Mapper.Context.class) {
                conf = ((Mapper.Context) obj).getConfiguration();
            } else {
                conf = ((Reducer.Context) obj).getConfiguration();
            }
            String coefficientLink = conf.get(Constants.COEFFICIENT_LINK, null);
            if (coefficientLink == null) {
                throw new AssertionError("coefficient file is not existed!");
            }
            featureSize = conf.getInt(Constants.FEATURE_SIZE, 0);
            if (featureSize <= 0) {
                throw new AssertionError("feature size error!");
            }
            coefficient = CacheAccess.readCoefficient(coefficientLink, featureSize);
            lossFunction = conf.get(Constants.LOSS_FUNCTION, null);
            if (lossFunction == null) {
                throw new AssertionError("loss function should be defined!");
            }
            reduceNumber = conf.getInt(Constants.REDUCE_NUMBER, 300);
            learningRate = conf.getDouble(Constants.LEARNING_RATE, 0.001);
            samplingRate = conf.getDouble(Constants.SAMPLING_RATE, 0.1);
        } catch (Exception e) {
            LOG.error("Class LocalContext function[parse] error:" + e);
        }
    }

    public double[] getCoefficient() {
        return coefficient;
    }

    public void setCoefficient(double[] coefficient) {
        this.coefficient = coefficient;
    }

    public String getLossFunction() {
        return lossFunction;
    }

    public void setLossFunction(String lossFunction) {
        this.lossFunction = lossFunction;
    }

    public int getReduceNumber() {
        return reduceNumber;
    }

    public void setReduceNumber(int reduceNumber) {
        this.reduceNumber = reduceNumber;
    }

    public double getLearningRate() {
        return learningRate;
    }

    public void setLearningRate(double learningRate) {
        this.learningRate = learningRate;
    }

    public int getFeatureSize() {
        return featureSize;
    }

    public void setFeatureSize(int featureSize) {
        this.featureSize = featureSize;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public void setSamplingRate(double samplingRate) {
        this.samplingRate = samplingRate;
    }
}
