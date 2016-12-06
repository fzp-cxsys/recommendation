package freagle.recommendation.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by freagle on 16-12-3.
 */
public class SparkSQLUtil {
    public static final SparkSession getOrCreateSparkSession(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("recommendation");
        SparkSession ss = SparkSession.builder().config(conf).getOrCreate();
        return ss;
    }

    public static final JavaSparkContext getJavaSparkContext(SparkSession ss){
        return new JavaSparkContext(ss.sparkContext());
    }

    public static final Dataset<Row> getJDBCDataFrame(SparkSession ss, String table){
        return ss.read().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", table, getDefaultMysqlProp());
    }

    public static final Dataset<Row> getJDBCDataFrame(String table){
        SparkSession ss = getOrCreateSparkSession();

        return ss.read().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", table, getDefaultMysqlProp());
    }

    public static final JavaRDD<String> getRDDFromDataFile(String dataFile){
        SparkSession ss = getOrCreateSparkSession();
        return ss.read().textFile("target/classes/ml-100k/" + dataFile).javaRDD();
    }

    public static final void writeDataFrameToJDBC(Dataset<Row> dataFrame, String table){
        dataFrame.write().jdbc("jdbc:mysql://127.0.0.1:3306/recommendation", table, getDefaultMysqlProp());
    }

    private static final Properties getDefaultMysqlProp(){
        Properties mysqlProp = new Properties();
        mysqlProp.setProperty("user", "root");
        mysqlProp.setProperty("password", "root");
        return mysqlProp;
    }

}
