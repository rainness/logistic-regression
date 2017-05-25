package com.github.rainness.model.loss;

import java.util.List;

/**
 * Created by rainness on 16-12-23.
 */
public class SquareRootLoss implements LossFunction {

    @Override
    public double fitness(double[] coefficient, List<Integer> featureList) {
        double score = 0.0;
        for (Integer pos : featureList) {
            score += coefficient[pos];
        }
        return score;
    }
}
