package freagle.recommendation.script;

import freagle.recommendation.bean.Rating;
import freagle.recommendation.util.SparkSQLUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Created by freagle on 16-12-3.
 */
public class InitDataSet {
    public static final void initUser(){

    }
    public static final void initItem(){

    }
    public static final void initRating(){
        JavaRDD<String> ratingRDD = SparkSQLUtil.getRDDFromDataFile("u.data");

        JavaRDD<Rating> ratings = ratingRDD.map(line -> {
            String[] splits = line.split("\t");

            Rating rating = new Rating();
            rating.setUser(Integer.parseInt(splits[0]));
            rating.setItem(Integer.parseInt(splits[1]));
            rating.setRating(Float.parseFloat(splits[2]));
            rating.setRatingTime(new Timestamp(Long.parseLong(splits[3]) * 1000));

            return rating;
        });

        SparkSession ss = SparkSQLUtil.getOrCreateSparkSession();
        Dataset<Row> ratingDataFrame = ss.createDataFrame(ratings, Rating.class);
        SparkSQLUtil.writeDataFrameToJDBC(ratingDataFrame, "rating");
    }

    public static void main(String[] args) {
//        initItem();
        initRating();
//        initUser();
//
        SparkSQLUtil.getOrCreateSparkSession().stop();
    }
}
