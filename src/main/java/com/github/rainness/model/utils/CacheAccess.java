package com.github.rainness.model.utils;

import com.google.common.io.LineReader;
import org.apache.log4j.Logger;
import java.io.FileReader;

/**
 * Created by rainness on 16-12-23.
 */
public class CacheAccess {

    private static Logger LOG = Logger.getLogger(CacheAccess.class);

    public static double[] readCoefficient(String coefficientPath, int featureSize) {
        double[] coefficient = new double[featureSize];
        try {
            LineReader reader = new LineReader(new FileReader(coefficientPath));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String[] array = line.split("\t");
                int index = Integer.parseInt(array[0]);
                double value = Double.parseDouble(array[1]);
                coefficient[index] = value;
            }
            return coefficient;
        } catch (Exception e) {
            LOG.error("Class CacheAccess function[readCoefficient] error:" + e);
            return coefficient;
        }
    }
}
