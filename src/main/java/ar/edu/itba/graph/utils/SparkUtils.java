package ar.edu.itba.graph.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkUtils {

    private final SparkConf sparkConf;
    private final JavaSparkContext sparkContext;

    public SparkUtils(String appName) {
        this.sparkConf = new SparkConf().setAppName(appName);
        this.sparkContext = new JavaSparkContext(sparkConf);
    }

    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();
    }

    public static void validatePath(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (path == null) {
            throw new IllegalArgumentException("Path must not be null.");
        }

        if (!fs.exists(path)) {
            throw new IOException("Path does not exist: " + path);
        }

        if (!fs.isFile(path)) {
            throw new IOException("Path is not a file: " + path);
        }
    }

    public static Dataset<Row> loadCSV(SparkSession session, String path) {
        return session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }
}
