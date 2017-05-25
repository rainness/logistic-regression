package com.github.rainness.model.iterator;

import com.github.rainness.model.utils.TrainUtils;
import com.github.rainness.model.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import java.net.URI;

/**
 * Created by rainness on 16-12-26.
 */
public class SGDTrainIteratorBuilder {

    private static Logger LOG = Logger.getLogger(SGDTrainIteratorBuilder.class);

    private Path inputDataPath;
    private Path outputDataPath;
    private Path coefficientDataPath;
    private Configuration conf;
    private String cacheLink;

    public SGDTrainIteratorBuilder(Path inputDataPath,
                                   Path outputDataPath, Path coefficientDataPath, Configuration conf) {
        this.inputDataPath = inputDataPath;
        this.outputDataPath = outputDataPath;
        this.coefficientDataPath = coefficientDataPath;
        this.cacheLink = "coefficient-cache-" + System.currentTimeMillis();
        conf.set(Constants.COEFFICIENT_LINK, cacheLink);
        this.conf = new Configuration(conf);
    }

    public void build() {
        try {
            Job job = Job.getInstance(conf, "SGDTrainIteratorJob");
            job.setJarByClass(SGDTrainIteratorBuilder.class);
            job.addCacheFile(new URI(coefficientDataPath.toString() + "#" + cacheLink));
            TextInputFormat.setInputPaths(job, TrainUtils.selectAsProption(conf, inputDataPath, 0.7).toArray(new Path[]{}));
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(SGDTrainIteratorViaMapper.class);
            job.setReducerClass(SGDTrainIteratorViaReducer.class);
            job.setPartitionerClass(IteratorShuffleKeyWritable.ShufflePartitioner.class);
            job.setGroupingComparatorClass(IteratorShuffleKeyWritable.ShuffleGroupingComparator.class);
            job.setMapOutputKeyClass(IteratorShuffleKeyWritable.class);
            job.setMapOutputValueClass(IteratorShuffleValueWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, outputDataPath);
            job.setNumReduceTasks(conf.getInt(Constants.REDUCE_NUMBER, 300));
            int exitCode = job.waitForCompletion(true) ? 0 : 1;
            if (exitCode != 0) {
                throw new AssertionError("run error");
            }
        } catch (Exception e) {
            throw new AssertionError("Class SGDTrainIteratorBuilder function[build] error:" + e);
        }
    }
}
