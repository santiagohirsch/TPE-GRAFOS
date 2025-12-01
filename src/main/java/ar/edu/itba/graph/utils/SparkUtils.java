package ar.edu.itba.graph.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static ar.edu.itba.graph.utils.ErrorUtils.*;

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
            throw new IllegalArgumentException(PATH_MUST_NOT_BE_NULL);
        }

        if (!fs.exists(path)) {
            throw new IOException(PATH_DOES_NOT_EXIST + path);
        }

        if (!fs.isFile(path)) {
            throw new IOException(PATH_IS_NOT_A_FILE + path);
        }
    }

    public static Dataset<Row> loadCSV(SparkSession session, String path) {
        return session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    public static void writeCSV(String content, String path, Configuration conf) throws IOException {
        Path p = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(p);
        try (FSDataOutputStream out = fs.create(p, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            writer.write(content);
        }
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }
}
