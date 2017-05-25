package com.github.rainness.model.judge;

import com.github.rainness.model.utils.Constants;
import com.github.rainness.model.utils.IoUtils;
import com.github.rainness.model.utils.Score;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by rainness on 16-12-26.
 */
public class SGDTrainJudgeDescent {

    private static Logger LOG = Logger.getLogger(SGDTrainJudgeDescent.class);

    public static double isDescent(Path inputPath, Path previousPath, Path latterPath, Configuration conf) {
        double[] previous = parseCoefficient(previousPath, conf);
        double[] latter = parseCoefficient(latterPath, conf);
        int featureSize = conf.getInt(Constants.FEATURE_SIZE, 0);
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        BufferedReader bin = null;
        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(inputPath);
            bin = new BufferedReader(new InputStreamReader(inputStream));
            double previousVal = 0.0;
            double latterVal = 0.0;
            int num = 0;
            while (true) {
                String line = bin.readLine();
                if (line == null) {
                    break;
                }
                String[] array = line.split("\t");
                double label;
                try {
                    label = Double.parseDouble(array[0]);
                } catch (Exception e) {
                    //ignore
                    label = array[0].equals("true") ? 1.0 : 0.0;
                }
                String[] feature = array[1].split("\\s");
                double tmpPrevious = 0.0;
                double tmpLatter = 0.0;
                for (String i : feature) {
                    int index = Integer.parseInt(i);
                    if (index >= featureSize) {
                        continue;
                    }
                    tmpPrevious += previous[Integer.parseInt(i)];
                    tmpLatter += latter[Integer.parseInt(i)];
                }
                previousVal += Math.pow(label - Score.sigmod(tmpPrevious), 2);
                latterVal += Math.pow(label - Score.sigmod(tmpLatter), 2);
                num++;
            }
            double previousCost = 1.0 / (2.0 * num) * previousVal;
            double latterCost = 1.0 / (2.0 * num) * latterVal;
            LOG.info("Class SGDTrainJudgeDescent function[isDescent]" + "previous cost is:" + previousCost + ", current cost is:" + latterCost);
            return latterCost - previousCost;
        } catch (Exception e) {
            LOG.error("Class SGDTrainJudgeDescent function[isDescent] error:" + e);
        } finally {
            IoUtils.closeQuietly(fs);
            IoUtils.closeQuietly(inputStream);
            IoUtils.closeQuietly(bin);
        }
        return 0.0;
    }

    private static double[] parseCoefficient(Path path, Configuration conf) {
        double[] coefficient = new double[conf.getInt(Constants.FEATURE_SIZE, 0)];
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        BufferedReader bin = null;
        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(path);
            bin = new BufferedReader(new InputStreamReader(inputStream));
            while (true) {
                String line = bin.readLine();
                if (line == null) {
                    break;
                }
                String[] array = line.split("\t");
                int index = Integer.parseInt(array[0]);
                double weight = Double.parseDouble(array[1]);
                coefficient[index] = weight;
            }
        } catch (Exception e) {
            LOG.error("Class SGDTrainJudgeDescent function[parseCoefficient] error:" + e);
        } finally {
            IoUtils.closeQuietly(fs);
            IoUtils.closeQuietly(inputStream);
            IoUtils.closeQuietly(bin);
        }
        return coefficient;
    }
}
