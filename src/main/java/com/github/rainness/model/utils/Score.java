package com.github.rainness.model.utils;

/**
 * Created by rainness on 16-12-23.
 */
public class Score {

    /* 目前feature的value采用one-hot-Encoding */
    public static double sigmod(double value) {
        return 1.0 / (1.0 + Math.exp(-1.0 * value));
    }
}
