package com.github.rainness.model.loss;

import java.util.List;

/**
 * Created by rainness on 16-12-23.
 */
public class DeltaLossCalculation {

    private LossFunction lossFunction;

    public double deltaLoss(double label, double[] coefficient, List<Integer> featureList, Class clazz) {
        if (clazz == LikeHoodLoss.class) {
            lossFunction = new LikeHoodLoss();
        } else if (clazz == SquareRootLoss.class) {
            lossFunction = new SquareRootLoss();
        } else {
            throw new AssertionError("unrecognized loss function!");
        }
        return label - lossFunction.fitness(coefficient, featureList);
    }
}
