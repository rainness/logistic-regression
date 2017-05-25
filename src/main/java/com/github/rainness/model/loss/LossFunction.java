package com.github.rainness.model.loss;

import java.util.List;

/**
 * Created by rainness on 16-12-23.
 */
public interface LossFunction {

    double fitness(double[] coefficient, List<Integer> featureList);
}
