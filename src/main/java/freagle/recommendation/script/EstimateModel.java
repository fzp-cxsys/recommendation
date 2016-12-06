package freagle.recommendation.script;

import freagle.recommendation.util.MLModelUtil;
import freagle.recommendation.util.SparkSQLUtil;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * Created by freagle on 16-12-4.
 */
public class EstimateModel {
    public static void main(String[] args) throws IOException {
        Dataset<Row> rating = SparkSQLUtil.getJDBCDataFrame("rating");
        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.01)
                .setUserCol("user")
                .setItemCol("item")
                .setRatingCol("rating");
        ALSModel model = als.fit(rating);


        MLModelUtil.saveModel(model, "u.model");

        SparkSQLUtil.getOrCreateSparkSession().stop();
    }
}
