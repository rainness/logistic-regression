package com.github.rainness.model;

import com.github.rainness.model.utils.DataAccess;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;

/**
 * Created by rainness on 16-12-27.
 */
public class TestHadoopLogisticRegression {

    @ClassRule
    public static final TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testHadoopLR() throws Exception {
        Path inputPath = new Path(tmpDir.newFolder("input.dir").getPath());
        Path outputPath = new Path(tmpDir.newFolder("output.dir").getPath());
        Path combinePath = new Path(tmpDir.newFolder("combine.dir").getPath());
        Path coefficientPath = new Path(tmpDir.newFolder("coefficient.dir").getPath());
        recursiveNewFile(inputPath);
        recursiveSimplyNewFile(coefficientPath);
        SGDTrainTool.main(
                "--input-data-path=" + inputPath,
                "--iterator-data-path=" + outputPath,
                "--combine-data-path=" + combinePath,
                "--coefficient-data-path=" + coefficientPath,
                "--learning-rate=" + 0.001,
                "--iterator-num=" + 100,
                "--reducer-num=" + 1,
                "--feature-size=" + 100);
    }

    private void recursiveNewFile(Path pathDir) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs;
            for (int i = 0; i < 10; i++) {
                Path childPath = new Path(pathDir, "part-r-0000" + i);
                fs = FileSystem.get(pathDir.toUri(), conf);
                fs.create(childPath);
                DataAccess.writeRandomData(DataAccess.SMART_VALUE.LOAD_FACTOR, new File(childPath.toString()),
                        5000, 5, 100, DataAccess.SMART_VALUE.TAB_SEPARATOR, DataAccess.SMART_VALUE.SPACE_SEPARATOR);
            }

        } catch (Exception e) {
            //ignore
        }
    }

    private void recursiveSimplyNewFile(Path pathDir) {
        try {
            Configuration conf = new Configuration();
            Path childPath = new Path(pathDir, "coefficient-0");
            FileSystem fs = FileSystem.get(pathDir.toUri(), conf);
            fs.create(childPath);
        } catch (Exception e) {
            //ignore
        }
    }
}
