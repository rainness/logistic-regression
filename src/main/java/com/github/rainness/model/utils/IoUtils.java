package com.github.rainness.model.utils;

import java.io.Closeable;

/**
 * Created by rainness on 17-1-12.
 */
public class IoUtils {

    public static void closeQuietly(Closeable obj) {
        try {
            if (obj != null) {
                obj.close();
            }
        } catch (Exception e) {
            //ignore
        }
    }
}
