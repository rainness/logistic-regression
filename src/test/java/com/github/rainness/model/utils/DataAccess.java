package com.github.rainness.model.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by rainness on 16-12-21.
 */
public class DataAccess {

    public interface SMART_VALUE {
        public static String TAB_SEPARATOR = "\t";
        public static String SPACE_SEPARATOR = " ";
        public static double LOAD_FACTOR = 0.8;
    }

    /**
     * 生成随机测试集，方便本地进行算法验证
     * @param dataFile
     * @param featureSize feature-size
     * @param sampleSize sample-size
     * @param factor 正负样本比例，一般取4，表示正负样本比例为1：4
     * @param columnSize 数据列数，For Example: true feature1-feature2-...-feature-n
     * @param columnSeparator 各数据之间分隔符
     * @param featureSeparator feature之间分隔符
     */
    public static void writeRandomData (
            double factor,
            File dataFile,
            int sampleSize,
            int columnSize,
            int featureSize,
            String columnSeparator,
            String featureSeparator) throws IOException {
        try {
            BufferedWriter bin = new BufferedWriter(new FileWriter(dataFile));
            Random random = new Random();
            List<String> sampleList = Lists.newArrayList();
            while (sampleSize > 0) {
                StringBuilder sb = new StringBuilder();
                if (random.nextDouble() < factor) {
                    sb.append("false" + columnSeparator);
                } else {
                    sb.append("true" + columnSeparator);
                }
                List<Integer> value = genRandomArray(featureSize);
                sb.append(StringUtils.join(value, featureSeparator));
                for (int i = 0; i < columnSize - 2; i++) {
                    sb.append(columnSeparator + new Integer(i).hashCode());
                }
                sampleList.add(sb.toString());
                sampleSize--;
            }
            Collections.shuffle(sampleList);
            for (String sample : sampleList) {
                bin.write(sample + "\n");
            }
            bin.flush();
            bin.close();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public static double calculateProption(File dir) {
        double positiveCnt = 0.0;
        double totalCnt = 0.0;
        try {
            List<File> fileList = Lists.newArrayList();
            if (dir.isDirectory()) {
                File[] files = dir.listFiles();
                fileList.addAll(Arrays.asList(files));
            } else {
                fileList.add(dir);
            }
            BufferedReader bin;
            for (File file : fileList) {
                bin = new BufferedReader(new FileReader(file));
                while (true) {
                    String line = bin.readLine();
                    if (line == null) {
                        break;
                    }
                    if (line.split("\t")[0].equals("1.0")) {
                        positiveCnt += 1.0;
                    }
                    totalCnt += 1.0;
                }
                bin.close();
            }
        } catch (Exception e) {
            throw new AssertionError("calculate proption error!");
        }
        return positiveCnt / totalCnt;
    }
    private static List<Integer> genRandomArray(int size) {
        int[] base = new int[size];
        List<Integer> value = Lists.newArrayList();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            base[i] = i;
        }
        int end = size - 1;
        for (int i = 0; i < size; i+= 2) {
            int pos = random.nextInt(end);
            value.add(base[pos]);
            base[pos] = base[end];
            end--;
        }
        return value;
    }
}
