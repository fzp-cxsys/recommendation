package freagle.recommendation.bin;

import freagle.recommendation.bean.Rating;
import freagle.recommendation.util.SparkSQLUtil;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;

import java.text.DateFormat;
import java.util.List;

/**
 * Created by freagle on 16-12-5.
 */
public class StartUp {
    public static void main(String[] args) {
        SparkSession ss = SparkSQLUtil.getOrCreateSparkSession();
        ALSModel model = ALSModel.load("target/classes/ml-100k/u.model");
        Dataset<Row> ratingDF = model.transform(SparkSQLUtil.getJDBCDataFrame("rating"));

        List<Row> ratings = ratingDF.
                where(ratingDF.col("user").equalTo(1)).
                orderBy(ratingDF.col("rating").desc(), ratingDF.col("item")).
                takeAsList(1000);

        for (Row rating : ratings) {
            System.out.print(rating.getInt(rating.fieldIndex("user")) + "|");
            System.out.print(rating.getInt(rating.fieldIndex("item")) + "|");
            System.out.print(rating.getDouble(rating.fieldIndex("rating")) + "|");
            System.out.print(rating.getFloat(rating.fieldIndex("prediction")) + "|");
            System.out.println(rating.getTimestamp(rating.fieldIndex("ratingTime")).toString());
        }
//        Rating[] result = (Rating[]) ratings.takeAsList(1000).toArray();

//        for (int i = 0; i < result.length; i++) {
//            System.out.print(result[i].getUser() + "|");
//            System.out.print(result[i].getItem() + "|");
//            System.out.print(result[i].getRating() + "|");
//            System.out.println(result[i].getRatingTime().toString());
//        }

        System.out.println(ratings.size());

        ss.stop();
    }
}
