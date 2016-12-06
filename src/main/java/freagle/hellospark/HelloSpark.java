package freagle.hellospark;

import freagle.recommendation.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by freagle on 16-11-29.
 */
public class HelloSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("HelloSparkSQL");
        SparkSession ss = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", "root");
        mysqlProp.setProperty("password", "root");


        Dataset<Row> userDataFrame = ss.read().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", "user", mysqlProp);

        Row firstUser = userDataFrame.first();

        System.out.println(firstUser.getInt(0));
        System.out.println(firstUser.getBoolean(1));
        System.out.println(userDataFrame.count());

//        将user数据从文件转移到mysql数据库

//        JavaRDD<String> users = jsc.textFile("target/classes/ml-100k/u.user");
//
//        HashMap<String, Integer> occuMapping = new HashMap<>();
//        occuMapping.put("administrator", 0);
//        occuMapping.put("artist", 1);
//        occuMapping.put("doctor", 2);
//        occuMapping.put("educator", 3);
//        occuMapping.put("engineer", 4);
//        occuMapping.put("entertainment", 5);
//        occuMapping.put("executive", 6);
//        occuMapping.put("healthcare", 7);
//        occuMapping.put("homemaker", 8);
//        occuMapping.put("lawyer", 9);
//        occuMapping.put("librarian", 10);
//        occuMapping.put("marketing", 11);
//        occuMapping.put("none", 12);
//        occuMapping.put("other", 13);
//        occuMapping.put("programmer", 14);
//        occuMapping.put("retired", 15);
//        occuMapping.put("salesman", 16);
//        occuMapping.put("scientist", 17);
//        occuMapping.put("student", 18);
//        occuMapping.put("technician", 19);
//        occuMapping.put("writer", 20);
//
//        JavaRDD<User> userRDD = users.map(line -> {
//            String[] splits = line.split("\\|");
//            User user = new User();
//            user.setUser(Integer.parseInt(splits[0]));
//            user.setAge(Integer.parseInt(splits[1]));
//            if (splits[2].equals("M")) {
//                user.setGender(false);
//            } else {
//                user.setGender(true);
//            }
//            user.setOccupation(occuMapping.get(splits[3]));
//            user.setZip_code(splits[4]);
//            return user;
//        });
//
//        Dataset<Row> userDataFrame = ss.createDataFrame(userRDD, User.class);
//
//        userDataFrame.write().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", "user", mysqlProp);

//        System.out.println(userRDD.count());

        ss.stop();
    }

}