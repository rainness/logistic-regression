package com.github.rainness.model;

import com.github.rainness.model.evalution.AucEvaluation;
import com.github.rainness.model.iterator.SGDTrainIteratorBuilder;
import com.github.rainness.model.judge.SGDTrainJudgeDescent;
import com.github.rainness.model.merge.SGDTrainCombineBuilder;
import com.github.rainness.model.utils.TrainUtils;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;
import com.github.rainness.model.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by rainness on 16-12-26.
 */
public class SGDTrainTool extends Configured implements Tool {

    private static Logger LOG = Logger.getLogger(SGDTrainTool.class);

    public interface CommandLines {

        @Option(longName = "input-data-path", defaultValue = "")
        String inputPath();

        @Option(longName = "iterator-data-path", defaultValue = "")
        String outputPath();

        @Option(longName = "combine-data-path", defaultValue = "")
        String combinePath();

        @Option(longName = "coefficient-data-path", defaultValue = "")
        String coefficientDataPath();

        @Option(longName = "learning-rate", defaultValue = "0.001")
        double learningRate();

        @Option(longName = "reducer-num", defaultValue = "50")
        int reducerNum();

        @Option(longName = "iterator-num", defaultValue = "100")
        int iteratorNum();

        @Option(longName = "feature-size", defaultValue = "10000000")
        int featureSize();

        @Option(longName = "loss-function", defaultValue = "com.github.rainness.model.loss.LikeHoodLoss", description = "other is com.github.rainness.model.loss.SquareRootLoss")
        String lossFunction();

        @Option(longName = "sampling-rate", defaultValue = "0.1")
        double samplingRate();
    }

    @Override
    public int run(String[] args) {
        try {
            Configuration conf = new Configuration();
            CommandLines cli = CliFactory.parseArguments(CommandLines.class, args);
            TrainUtils.newLocalConfiguration(conf, cli);
            Path inputDataPath = new Path(cli.inputPath());
            Path outputDataPath = new Path(cli.outputPath());
            Path combineDataPath = new Path(cli.combinePath());
            Path coefficientDataPath = new Path(cli.coefficientDataPath());
            TrainUtils.recursiveSimplyNewFile(coefficientDataPath);
            boolean skip;
            boolean isConvergence = false;
            for (int i = 0; i < cli.iteratorNum(); i++) {
                // execute iterator for each step
                Path iteratorStagePath = new Path(outputDataPath, "iterator-" + (i + 1) + ".success");
                skip = Stage.stageSuccess(conf, iteratorStagePath);
                if (!skip && !isConvergence) {
                    LOG.info("SGD Train iterator with " + (i + 1) + " times!");
                    Path iteratorInputPath = inputDataPath;
                    Path iteratorOutputPath = new Path(outputDataPath, "iterator-" + (i + 1));
                    Path iteratorCoefficientPath = new Path(coefficientDataPath, "coefficient-" + i);
                    SGDTrainIteratorBuilder builder =
                            new SGDTrainIteratorBuilder(iteratorInputPath, iteratorOutputPath, iteratorCoefficientPath, conf);
                    builder.build();
                    Stage.stageUpdate(conf, iteratorStagePath);
                }
                //merge all coefficient on each reducer
                Path mergeStagePath = new Path(combineDataPath, "combine-" + (i + 1) + ".success");
                skip = Stage.stageSuccess(conf, mergeStagePath);
                if (!skip && !isConvergence) {
                    LOG.info("SGD Train combine with " + (i + 1) + " times!");
                    Path combineInputPath = new Path(outputDataPath, "iterator-" + (i + 1));
                    Path combineOutputPath = new Path(combineDataPath, "combine-" + (i + 1));
                    SGDTrainCombineBuilder builder =
                            new SGDTrainCombineBuilder(combineInputPath, combineOutputPath, conf);
                    builder.build();
                    Stage.stageUpdate(conf, mergeStagePath);
                    TrainUtils.mv(conf, new Path(combineOutputPath, "part-r-00000"), new Path(coefficientDataPath, "coefficient-" + (i + 1)));
                } else {
                    LOG.info("SGD Train go to next step because current step " + (i + 1) + " has been accomplished history!");
                    continue;
                }
                //Judge whether model is convergence, select the first file to judge
                Path baselineInputPath = TrainUtils.selectAsProption(conf, inputDataPath, 0.7).get(0);
                Path previousCoefficientPath = new Path(coefficientDataPath, "coefficient-" + i);
                Path currentCoefficientPath = new Path(coefficientDataPath, "coefficient-" + (i + 1));
                double deltaCost = SGDTrainJudgeDescent.isDescent(baselineInputPath, previousCoefficientPath, currentCoefficientPath, conf);
                if (Math.abs(deltaCost) < 0.0001) {
                    LOG.info("SGD Train finished at " + (i + 1) + " times!");
                    isConvergence = true;
                } else {
                    double learningRate = conf.getDouble(Constants.LEARNING_RATE, 0.001);
                    if (deltaCost > 0) {
                        //update learningRate, 减小步长, 返回到上一次迭代
                        /** need to be checked **/
                        TrainUtils.lookBack(conf, new Path(outputDataPath, "iterator-" + (i + 1) + ".success"), i);
                        TrainUtils.lookBack(conf, new Path(combineDataPath, "combine-" + (i + 1) + ".success"), i);
                        TrainUtils.lookBack(conf, new Path(outputDataPath, "iterator-" + (i + 1)), i);
                        TrainUtils.lookBack(conf, new Path(combineDataPath, "combine-" + (i + 1)), i);
                        TrainUtils.lookBack(conf, new Path(coefficientDataPath, "coefficient-" + (i + 1)), i);
                        LOG.info("Unfortunately, SGD Train will go back from step " + (i + 1) + " to step " + i);
                        //update learningRate, 减小步长
                        learningRate /= Math.sqrt(i + 2);
                        i -= 1;
                    } else {
                        //update learningRate, 增大步长
                        learningRate *= 3;
                    }
                    conf.setDouble(Constants.LEARNING_RATE, learningRate);
                }
                if (isConvergence || i == cli.iteratorNum() - 1) {
                    //calculate AUC
                    Path testDataPath = TrainUtils.selectForTesting(conf, inputDataPath, 0.7).get(0);
                    Path coefficientPath = new Path(coefficientDataPath, "coefficient-" + (i + 1));
                    Path uselessOutputPath = new Path(combineDataPath, "useless");
                    AucEvaluation evaluation = new AucEvaluation(coefficientPath, testDataPath, uselessOutputPath, conf);
                    double auc = evaluation.evaluate();
                    if (auc == 0.0) {
                        throw new AssertionError("auc calculation error");
                    } else {
                        LOG.info("SGD Train finished with AUC: " + auc + " at " + (i + 1) + " times !");
                    }
                    break;
                }
            }
        } catch (Exception e) {
            throw new AssertionError("LR train with sgd error:" + e);
        }
        return 0;
    }

    public static void main(String... args) {
        try {
            ToolRunner.run(new SGDTrainTool(), args);
        } catch (Exception e) {
            throw new AssertionError("SGDTrain error!");
        }
    }
}
