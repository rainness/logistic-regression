package com.github.rainness.model.iterator;

import com.github.rainness.model.loss.DeltaLossCalculation;
import com.github.rainness.model.utils.LocalContext;
import com.github.rainness.model.utils.SampleWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.util.Random;

/**
 * Created by rainness on 16-12-23.
 */
public class SGDTrainIteratorViaMapper extends Mapper<Writable, Text, IteratorShuffleKeyWritable, IteratorShuffleValueWritable> {

    private static Logger LOG = Logger.getLogger(SGDTrainIteratorViaMapper.class);
    //选取样本中部分样本进行梯度下降
    private double samplingRate = 1.0;
    private DeltaLossCalculation deltaUtils = new DeltaLossCalculation();
    private Random random = new Random();
    private SampleWrapper wrapper = new SampleWrapper();
    private LocalContext localContext = new LocalContext();
    private double[] coefficient;
    private String lossFunction;
    private int featureSize;
    private IteratorShuffleKeyWritable intWritable = new IteratorShuffleKeyWritable();
    private IteratorShuffleValueWritable valueWritable = new IteratorShuffleValueWritable();

    @Override
    public void setup(Context context) {
        localContext.parse(context, Context.class);
        coefficient = localContext.getCoefficient();
        lossFunction = localContext.getLossFunction();
        featureSize = localContext.getFeatureSize();
        samplingRate = localContext.getSamplingRate();
    }
    @Override
    public void map(Writable ignored, Text value, Context context) {
        try {
            if (random.nextDouble() > samplingRate) {
                return;
            }
            SampleWrapper.Sample sample = wrapper.parse(value, featureSize);
            double delta = deltaUtils.deltaLoss(sample.getLabel(), coefficient, sample.getFeature(), Class.forName(lossFunction));
            intWritable.set(random.nextInt(localContext.getReduceNumber()));
            valueWritable.setDelta(delta);
            valueWritable.setFeature(StringUtils.join(sample.getFeature(), ","));
            context.write(intWritable, valueWritable);
        } catch (Exception e) {
            LOG.error("Class SGDTrainViaMapper function[map] error:" + e);
        }
    }
}
