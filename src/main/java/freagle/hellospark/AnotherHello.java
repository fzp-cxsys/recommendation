package freagle.hellospark;

import freagle.recommendation.bean.Item;
import freagle.recommendation.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Created by freagle on 16-12-3.
 */
public class AnotherHello {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("HelloSparkSQL");
        SparkSession ss = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", "root");
        mysqlProp.setProperty("password", "root");
//        将Item数据从文件转移到mysql数据库

        JavaRDD<String> items = jsc.textFile("target/classes/ml-100k/u.item");

        JavaRDD<Item> itemRDD = items.map(line -> {
            Item item = new Item();
            String[] splits = line.split("\\|");
            String[] genreSplits = Arrays.copyOfRange(splits, 5, splits.length);
            ArrayList<String> genres = new ArrayList<>(genreSplits.length);

            for (int i = 0; i < genreSplits.length; i++) {
                if(!genreSplits[i].equals("0")){
                    genres.add("" + i);
                }
            }

            String genreStr = String.join(",", genres);

            item.setGenres(genreStr);
            item.setItem(Integer.parseInt(splits[0]));
            item.setTitle(splits[1]);
            item.setUrl(splits[4]);

            return item;
        });

        Dataset<Row> itemDataFrame = ss.createDataFrame(itemRDD, Item.class);

        itemDataFrame.write().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", "item", mysqlProp);

        System.out.println(itemRDD.count());
    }
}
