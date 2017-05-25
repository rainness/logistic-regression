package com.github.rainness.model.utils;

import com.clearspring.analytics.util.Lists;
import com.github.rainness.model.SGDTrainTool;
import com.github.rainness.model.iterator.SGDTrainIteratorViaMapper;
import com.github.rainness.model.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;


/**
 * Created by rainness on 16-12-26.
 */
public class TrainUtils {

    private static Logger LOG = Logger.getLogger(SGDTrainIteratorViaMapper.class);

    public static List<Path> selectAsProption(Configuration conf, Path inputPath, double proption) {
        List<Path> results = Lists.newArrayList();
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] statuses = fs.listStatus(inputPath);
            for (int i = 0; i < statuses.length * proption; i++) {
                if (statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                    continue;
                }
                results.add(statuses[i].getPath());
            }
        } catch (Exception e) {
            LOG.error("Class TrainUtils function[selectAsProption] error:" + e);
        }
        return results;
    }

    public static List<Path> selectForTesting(Configuration conf, Path inputPath, double proption) {
        List<Path> results = Lists.newArrayList();
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] statuses = fs.listStatus(inputPath);
            for (int i = (int) (statuses.length * proption); i < statuses.length; i++) {
                if (statuses[i].getPath().toString().endsWith("_SUCCESS")) {
                    continue;
                }
                results.add(statuses[i].getPath());
            }
        } catch (Exception e) {
            LOG.error("Class TrainUtils function[selectForTesting] error:" + e);
        }
        return results;
    }

    public static void mv(Configuration conf, Path input, Path output) {
        try {
            FileSystem fs = FileSystem.get(conf);
            InputStream in = fs.open(input);
            OutputStream out = fs.create(output);
            IOUtils.copyBytes(in, out, conf);
        } catch (Exception e) {
            LOG.error("Class TrainUtils function[mv] error:" + e);
        }
    }

    public static void newLocalConfiguration(Configuration conf, SGDTrainTool.CommandLines cli) {
        try {
            conf.setInt(Constants.FEATURE_SIZE, cli.featureSize());
            conf.setDouble(Constants.LEARNING_RATE, cli.learningRate());
            conf.set(Constants.LOSS_FUNCTION, cli.lossFunction());
            conf.setInt(Constants.REDUCE_NUMBER, cli.reducerNum());
            conf.setDouble(Constants.SAMPLING_RATE, cli.samplingRate());
        } catch (Exception e) {
            throw new AssertionError("parse commandline error");
        }
    }

    public static void lookBack(Configuration conf, Path deletePath, int i) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(deletePath, true);
        } catch (Exception e) {
            throw new AssertionError("SGD Train go back to step " + i + " error:" + e);
        }
    }

    public static void recursiveSimplyNewFile(Path pathDir) {
        try {
            Configuration conf = new Configuration();
            Path childPath = new Path(pathDir, "coefficient-0");
            FileSystem fs = FileSystem.get(pathDir.toUri(), conf);
            if (!fs.exists(childPath)) {
                fs.create(childPath);
            }
        } catch (Exception e) {
            throw new AssertionError("create new file error");
        }
    }
}
