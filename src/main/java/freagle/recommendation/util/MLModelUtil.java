package freagle.recommendation.util;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLWritable;

import java.io.IOException;

/**
 * Created by freagle on 16-12-5.
 */
public class MLModelUtil {
    public static final void saveModel(MLWritable model, String filename) throws IOException {
        model.write().save("target/classes/ml-100k/" + filename);
    }
}
