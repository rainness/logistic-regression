package com.github.rainness.model.loss;

import java.util.List;

/**
 * Created by rainness on 16-12-23.
 */
public class LikeHoodLoss implements LossFunction {

    @Override
    public double fitness(double[] coefficient, List<Integer> featureList) {
        double score = 0.0;
        for (Integer pos : featureList) {
            score += coefficient[pos];
        }
        return 1.0 / (1.0 + Math.exp(-1.0 * score));
    }
}
