package ar.edu.itba.graph.utils;

import org.apache.spark.sql.Row;

import java.util.List;

public class GraphFrameUtils {

    public static String graphToString(List<Row> rows, String[] columns) {
        StringBuilder sb = new StringBuilder();

        sb.append(columns[0])
          .append(",")
          .append(columns[1])
          .append("\n");

        for (Row row : rows) {
            sb.append(row.get(0).toString())
              .append(",")
              .append(row.get(1).toString())
              .append("\n");
        }
        return sb.toString();
    }
}
