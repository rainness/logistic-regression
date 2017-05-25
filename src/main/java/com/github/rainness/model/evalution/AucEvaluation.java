package com.github.rainness.model.evalution;

import com.github.rainness.model.iterator.SGDTrainIteratorViaMapper;
import com.github.rainness.model.loss.LikeHoodLoss;
import com.github.rainness.model.loss.LossFunction;
import com.github.rainness.model.loss.SquareRootLoss;
import com.github.rainness.model.utils.LocalContext;
import com.github.rainness.model.utils.SampleWrapper;
import com.google.common.collect.Lists;
import com.github.rainness.model.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.List;


/**
 * Created by rainness on 16-12-28.
 */
public class AucEvaluation implements Evaluation {

    private static Logger LOG = Logger.getLogger(SGDTrainIteratorViaMapper.class);

    private static class EvaluateShuffleKeyWritable implements Writable {

        private IntWritable intWritable = new IntWritable();
        private ByteWritable byteWritable = new ByteWritable();
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

        public ByteWritable getByteWritable() {
            return byteWritable;
        }

        public void setByteWritable(byte byteWritable) {
            this.byteWritable.set(byteWritable);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            intWritable.write(dataOutput);
            byteWritable.write(dataOutput);
            doubleWritable.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            intWritable.readFields(dataInput);
            byteWritable.readFields(dataInput);
            doubleWritable.readFields(dataInput);
        }
    }

    private static class EvaluateShuffleValueWritable implements Writable {

        private ByteWritable byteWritable = new ByteWritable();
        private DoubleWritable doubleWritable = new DoubleWritable();

        public ByteWritable getByteWritable() {
            return byteWritable;
        }

        public void setByteWritable(byte byteWritable) {
            this.byteWritable.set(byteWritable);
        }

        public DoubleWritable getDoubleWritable() {
            return doubleWritable;
        }

        public void setDoubleWritable(double doubleWritable) {
            this.doubleWritable.set(doubleWritable);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            byteWritable.write(dataOutput);
            doubleWritable.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            byteWritable.readFields(dataInput);
            doubleWritable.readFields(dataInput);
        }
    }

    private static class ShufflePartitioner extends Partitioner<EvaluateShuffleKeyWritable, EvaluateShuffleValueWritable> {

        @Override
        public int getPartition(EvaluateShuffleKeyWritable key, EvaluateShuffleValueWritable value, int numPartitions) {
            return key.getIntWritable().get() % numPartitions;
        }
    }

    private static class ShuffleGroupingComparator implements RawComparator<EvaluateShuffleKeyWritable> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return IntWritable.Comparator.compareBytes(b1, s1, 4, b2, s2, 4);
        }

        @Override
        public int compare(EvaluateShuffleKeyWritable o1, EvaluateShuffleKeyWritable o2) {
            return Integer.compare(o1.getIntWritable().get(), o2.getIntWritable().get());
        }
    }

    private static class ShuffleSortingComparator implements RawComparator<EvaluateShuffleKeyWritable> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int cmp = IntWritable.Comparator.compareBytes(b1, s1, 4, b2, s2, 4);
            if (cmp == 0) {
                //负样本排在前
                cmp = ByteWritable.Comparator.compareBytes(b1, s1 + 4, 1, b2, s2 + 4, 1);
                if (cmp == 0) {
                    //按照score升序
                    cmp = DoubleWritable.Comparator.compareBytes(b1, s1 + 5, 8, b2, s2 + 5, 8);
                }
            }
            return cmp;
        }

        @Override
        public int compare(EvaluateShuffleKeyWritable o1, EvaluateShuffleKeyWritable o2) {
            int cmp = Integer.compare(o1.getIntWritable().get(), o2.getIntWritable().get());
            if (cmp == 0) {
                cmp = Byte.compare(o1.getByteWritable().get(), o2.getByteWritable().get());
                if (cmp == 0) {
                    cmp = Double.compare(o1.getDoubleWritable().get(), o2.getDoubleWritable().get());
                }
            }
            return cmp;
        }
    }

    private Path coefficientPath;
    private Path baseEvaluationPath;
    private Path outputDataPath;
    private Configuration conf;
    private String cacheLink;

    public AucEvaluation(Path coefficientPath, Path baseEvaluationPath, Path outputDataPath, Configuration conf) {
        this.coefficientPath = coefficientPath;
        this.baseEvaluationPath = baseEvaluationPath;
        this.outputDataPath = outputDataPath;
        this.cacheLink = "coefficient-cache-" + System.currentTimeMillis();
        conf.set(Constants.COEFFICIENT_LINK, cacheLink);
        this.conf = new Configuration(conf);
    }

    public static class AucEvaluationMapper extends Mapper<Writable, Text, EvaluateShuffleKeyWritable, EvaluateShuffleValueWritable> {

        private static Logger LOG = Logger.getLogger(AucEvaluationMapper.class);
        private EvaluateShuffleKeyWritable shuffleKeyWritable = new EvaluateShuffleKeyWritable();
        private EvaluateShuffleValueWritable shuffleValueWritable = new EvaluateShuffleValueWritable();
        private LossFunction fitness;
        private LocalContext localContext = new LocalContext();
        private SampleWrapper wrapper = new SampleWrapper();
        private double[] coefficient;
        private int featureSize;

        @Override
        public void setup(Context context) {
            try {
                localContext.parse(context, Context.class);
                coefficient = localContext.getCoefficient();
                featureSize = localContext.getFeatureSize();
                String name = localContext.getLossFunction();
                if (Class.forName(name) == LikeHoodLoss.class) {
                    fitness = LikeHoodLoss.class.newInstance();
                } else if (Class.forName(name) == SquareRootLoss.class) {
                    fitness = SquareRootLoss.class.newInstance();
                } else {
                    throw new AssertionError("unrecognized fitness function");
                }
            } catch (Exception e) {
                LOG.error("Class AucEvaluationMapper function[setup] error:" + e);
            }
        }

        @Override
        public void map(Writable ignored, Text value, Context context) {
            try {
                SampleWrapper.Sample sample = wrapper.parse(value, featureSize);
                byte label = sample.getLabel() == 1.0 ? (byte)1 : (byte)0;
                double score = fitness.fitness(coefficient, sample.getFeature());
                shuffleKeyWritable.setIntWritable(1);
                shuffleKeyWritable.setByteWritable(label);
                shuffleKeyWritable.setDoubleWritable(score);
                shuffleValueWritable.setByteWritable(label);
                shuffleValueWritable.setDoubleWritable(score);
                context.write(shuffleKeyWritable, shuffleValueWritable);
            } catch (Exception e) {
                LOG.error("Class AucEvaluationMapper function[map] error:" + e);
            }
        }
    }

    public static class AucEvaluationReducer extends Reducer<EvaluateShuffleKeyWritable, EvaluateShuffleValueWritable, NullWritable, NullWritable> {

        private static Logger LOG = Logger.getLogger(AucEvaluationReducer.class);
        private List<Double> negativeScoreList = Lists.newLinkedList();
        private Double[] negativeScoreArray;
        private Counter positive;
        private Counter negative;
        private Counter passed;
        private Counter samed;

        @Override
        public void setup(Context context) {
            passed = context.getCounter("com.github.rainness.model", "passed.number");
            positive = context.getCounter("com.github.rainness.model", "positive.number");
            negative = context.getCounter("com.github.rainness.model", "negative.number");
            samed = context.getCounter("com.github.rainness.model", "samed.number");
        }

        @Override
        public void reduce(EvaluateShuffleKeyWritable key, Iterable<EvaluateShuffleValueWritable> values, Context context) {
            try {
                for (EvaluateShuffleValueWritable valueWritable : values) {
                    switch (valueWritable.getByteWritable().get()) {
                        case 0:
                            negativeScoreList.add(valueWritable.getDoubleWritable().get());
                            negative.increment(1);
                            break;
                        case 1:
                            if (negativeScoreArray == null) {
                                negativeScoreArray = negativeScoreList.toArray(new Double[0]);
                            }
                            double value = valueWritable.getDoubleWritable().get();
                            int[] array = search(negativeScoreArray, 0, negativeScoreArray.length, value);
                            passed.increment(array[0]);
                            samed.increment(array[1]);
                            positive.increment(1);
                            break;
                    }
                }
            } catch (Exception e) {
                LOG.error("Class AucEvaluationReducer function[map] error:" + e);
            }
        }

        private int[] search(Double[] negativeScoreArray, int start, int end, double value) {
            //binary search
            while (start < end) {
                int mid = (start + end) / 2;
                if (negativeScoreArray[mid] < value) {
                    start = mid + 1;
                } else if (negativeScoreArray[mid] > value) {
                    end = mid - 1;
                } else {
                    int size = 0;
                    int startIndex = mid;
                    //linear search
                    while (startIndex < end) {
                        if (negativeScoreArray[startIndex] == value) {
                            size++;
                        } else {
                            break;
                        }
                    }
                    return new int[]{mid, size};
                }
            }
            return new int[] {start, 0};
        }
    }

    public double evaluate() {
        try {
            Job job = Job.getInstance(conf, "SGDTrainAucEvaluationJob");
            job.setJarByClass(AucEvaluation.class);
            job.addCacheFile(new URI(coefficientPath.toString() + "#" + cacheLink));
            TextInputFormat.setInputPaths(job, baseEvaluationPath);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(AucEvaluationMapper.class);
            job.setReducerClass(AucEvaluationReducer.class);
            job.setMapOutputKeyClass(EvaluateShuffleKeyWritable.class);
            job.setMapOutputValueClass(EvaluateShuffleValueWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setPartitionerClass(ShufflePartitioner.class);
            job.setGroupingComparatorClass(ShuffleGroupingComparator.class);
            job.setSortComparatorClass(ShuffleSortingComparator.class);
            TextOutputFormat.setOutputPath(job, outputDataPath);
            job.setNumReduceTasks(1);
            int exitCode = job.waitForCompletion(true) ? 0 : 1;
            if (exitCode != 0) {
                throw new AssertionError("run error");
            }
            Counter positive = job.getCounters().findCounter("com.github.rainness.model", "positive.number");
            Counter negative = job.getCounters().findCounter("com.github.rainness.model", "negative.number");
            Counter pass = job.getCounters().findCounter("com.github.rainness.model", "passed.number");
            Counter same = job.getCounters().findCounter("com.github.rainness.model", "samed.number");
            return ((double) pass.getValue() / positive.getValue() + 0.5 * same.getValue()) / negative.getValue();
        } catch (Exception e) {
            LOG.error("Class AucEvaluation function[evaluate] error:" + e);
        }
        return 0.0;
    }
}
