package com.github.rainness.model.merge;

import com.github.rainness.model.iterator.SGDTrainIteratorBuilder;
import com.github.rainness.model.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rainness on 16-12-26.
 */
public class SGDTrainCombineBuilder {

    private static Logger LOG = Logger.getLogger(SGDTrainCombineBuilder.class);

    private Path inputDataPath;
    private Path outputDataPath;
    private Configuration conf;

    public SGDTrainCombineBuilder(Path inputDataPath, Path outputDataPath, Configuration conf) {
        this.inputDataPath = inputDataPath;
        this.outputDataPath = outputDataPath;
        this.conf = new Configuration(conf);
    }

    public static class SGDTrainCombineViaMapper extends Mapper<IntWritable, DoubleWritable, IntWritable, CombineShuffleValueWritable> {

        private CombineShuffleKeyWritable intWritable = new CombineShuffleKeyWritable();
        private CombineShuffleValueWritable valueWritable = new CombineShuffleValueWritable();

        @Override
        public void map(IntWritable key, DoubleWritable value, Context context) {
            try {
                if (key == null || value == null) {
                    return;
                }
                valueWritable.setIntWritable(key.get());
                valueWritable.setDoubleWritable(value.get());
                context.write(intWritable, valueWritable);
            } catch (Exception e) {
                LOG.error("Class SGDTrainCombineViaMapper function[map] error:" + e);
            }
        }
    }

    public static class SGDTrainCombineViaReducer extends Reducer<IntWritable, CombineShuffleValueWritable, IntWritable, DoubleWritable> {

        private Map<Integer, Double> valueMap = new HashMap();
        private IntWritable intWritable = new IntWritable();
        private DoubleWritable doubleWritable = new DoubleWritable();
        private int reducerNum = 0;

        @Override
        public void setup(Context context) {
            reducerNum = context.getConfiguration().getInt(Constants.REDUCE_NUMBER, 300);
        }

        @Override
        public void reduce(IntWritable key, Iterable<CombineShuffleValueWritable> values, Context context) {
            try {
                for (CombineShuffleValueWritable value : values) {
                    update(valueMap, value.getIntWritable().get(), value.getDoubleWritable().get());
                }
                for (int i : valueMap.keySet()) {
                    intWritable.set(i);
                    doubleWritable.set(valueMap.get(i) / reducerNum);
                    context.write(intWritable, doubleWritable);
                }
            } catch (Exception e) {
                LOG.error("Class SGDTrainCombineViaReducer function[reduce] error:" + e);
            }
        }

        private void update(Map<Integer, Double> valueMap, int index, double value) {
            if (valueMap.containsKey(index)) {
                double tmp = valueMap.get(index) + value;
                valueMap.put(index, tmp);
            } else {
                valueMap.put(index, value);
            }
        }
    }

    public void build() {
        try {
            Job job = Job.getInstance(conf, "SGDTrainCombineJob");
            job.setJarByClass(SGDTrainIteratorBuilder.class);
            SequenceFileInputFormat.setInputPaths(job, inputDataPath);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(SGDTrainCombineViaMapper.class);
            job.setReducerClass(SGDTrainCombineViaReducer.class);
            job.setMapOutputKeyClass(CombineShuffleKeyWritable.class);
            job.setMapOutputValueClass(CombineShuffleValueWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, outputDataPath);
            job.setNumReduceTasks(1);
            int exitCode = job.waitForCompletion(true) ? 0 : 1;
            if (exitCode != 0) {
                throw new AssertionError("run error");
            }
        } catch (Exception e) {
            throw new AssertionError("Class SGDTrainCombineBuilder function[build] error:" + e);
        }
    }
}
