package com.github.rainness.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by rainness on 16-12-26.
 */
public class Stage {

    public static boolean stageSuccess(Configuration conf, Path path) {
        try {
            FileSystem fs = FileSystem.get(conf);
            return fs.exists(path);
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean stageUpdate(Configuration conf, Path path) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.create(path);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
