package ar.edu.itba.graph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import ar.edu.itba.graph.utils.SparkUtils;

import java.io.IOException;
import java.util.Date;

import static ar.edu.itba.graph.utils.ErrorUtils.*;
import static ar.edu.itba.graph.utils.GraphFrameUtils.graphToString;
import static ar.edu.itba.graph.utils.MainUtils.*;
import static ar.edu.itba.graph.utils.SparkUtils.*;
import static ar.edu.itba.graph.utils.VarsUtils.*;

public class KCore {

    public static void main(String[] args) throws IOException {
        validateArguments(args);

        SparkUtils sparkUtils = new SparkUtils(APP_NAME);

        SparkSession session = sparkUtils.getSparkSession();

        Configuration conf = session.sparkContext().hadoopConfiguration();

        String verticesArg = args[0];
        Path verticesPath = new Path(verticesArg);
        validatePath(verticesPath, conf);

        String edgesArg = args[1];
        Path edgesPath = new Path(edgesArg);
        validatePath(edgesPath, conf);

        Dataset<Row> verticesDF = loadCSV(session, verticesArg);
        verticesDF = verticesDF
                .withColumnRenamed(VERTICES_ID_COL, GRAPHFRAMES_ID_COL);

        Dataset<Row> edgesDF = loadCSV(session, edgesArg);
        edgesDF = edgesDF
                .withColumnRenamed(EDGES_SRC_COL, GRAPHFRAMES_SRC_COL)
                .withColumnRenamed(EDGES_DST_COL, GRAPHFRAMES_DST_COL);

        KCore kCore = new KCore();
        if (!kCore.isValidGraph(edgesDF)) {
            throwError(UNDERLYING_STRUCTURE_NOT_VALID);
        }

        Dataset<Row> reversed = edgesDF.select(
                edgesDF.col(GRAPHFRAMES_DST_COL).alias(GRAPHFRAMES_SRC_COL),
                edgesDF.col(GRAPHFRAMES_SRC_COL).alias(GRAPHFRAMES_DST_COL)
        );
        edgesDF = edgesDF.union(reversed).distinct();

        GraphFrame graph = GraphFrame.apply(verticesDF, edgesDF);

        int k = Integer.parseInt(args[2]);

        GraphFrame result = kCore.decomposition(graph, k);

        Date now = new Date();
        String timestamp = String.valueOf(now.getTime());

        writeCSV(
                graphToString(result.vertices().orderBy(GRAPHFRAMES_ID_COL).collectAsList(), result.vertexColumns()),
                timestamp + VERTICES_EXTENSION,
                conf
        );
        writeCSV(
                graphToString(result.edges().orderBy(GRAPHFRAMES_SRC_COL, GRAPHFRAMES_DST_COL).collectAsList(), result.edgeColumns()),
                timestamp + EDGES_EXTENSION,
                conf
        );

        sparkUtils.getSparkContext().close();
    }

    private GraphFrame decomposition(GraphFrame graph, int k) {
        boolean changed;
        GraphFrame currentGraph = graph;

        do {
            Dataset<Row> edges = currentGraph.edges();

            Dataset<Row> degrees = edges
                    .select(edges.col(GRAPHFRAMES_SRC_COL).as(GRAPHFRAMES_ID_COL))
                    .union(edges.select(edges.col(GRAPHFRAMES_DST_COL).as(GRAPHFRAMES_ID_COL)))
                    .groupBy(GRAPHFRAMES_ID_COL)
                    .count()
                    .withColumnRenamed(COUNT, GRAPHFRAMES_DEGREE_COL);

            Dataset<Row> verticesNoDegree = currentGraph.vertices().drop(GRAPHFRAMES_DEGREE_COL);

            Dataset<Row> filteredVertices = verticesNoDegree
                    .join(degrees, verticesNoDegree.col(GRAPHFRAMES_ID_COL).equalTo(degrees.col(GRAPHFRAMES_ID_COL)), INNER)
                    .drop(degrees.col(GRAPHFRAMES_ID_COL))
                    .filter(degrees.col(GRAPHFRAMES_DEGREE_COL).geq(k))
                    .select(verticesNoDegree.col(GRAPHFRAMES_ID_COL), verticesNoDegree.col(VERTICES_NAME_COL));

            Dataset<Row> srcIds = filteredVertices.select(filteredVertices.col(GRAPHFRAMES_ID_COL).alias(GRAPHFRAMES_SRC_ALIAS));
            Dataset<Row> dstIds = filteredVertices.select(filteredVertices.col(GRAPHFRAMES_ID_COL).alias(GRAPHFRAMES_DST_ALIAS));

            Dataset<Row> filteredEdges = edges
                    .join(srcIds, edges.col(GRAPHFRAMES_SRC_COL).equalTo(srcIds.col(GRAPHFRAMES_SRC_ALIAS)), INNER)
                    .join(dstIds, edges.col(GRAPHFRAMES_DST_COL).equalTo(dstIds.col(GRAPHFRAMES_DST_ALIAS)), INNER)
                    .select(edges.col(GRAPHFRAMES_SRC_COL), edges.col(GRAPHFRAMES_DST_COL));

            GraphFrame newGraph = GraphFrame.apply(filteredVertices, filteredEdges);

            changed = newGraph.vertices().count() != currentGraph.vertices().count();
            currentGraph = newGraph;

        } while (changed);

        return currentGraph;
    }

    private boolean isValidGraph(Dataset<Row> edges) {
        Dataset<Row> reversedEdges = edges.select(
                edges.col(GRAPHFRAMES_DST_COL).alias(GRAPHFRAMES_SRC_COL),
                edges.col(GRAPHFRAMES_SRC_COL).alias(GRAPHFRAMES_DST_COL)
        );
        long multigraphViolations= edges.join(reversedEdges,
                edges.col(GRAPHFRAMES_SRC_COL).equalTo(reversedEdges.col(GRAPHFRAMES_SRC_COL))
                        .and(edges.col(GRAPHFRAMES_DST_COL).equalTo(reversedEdges.col(GRAPHFRAMES_DST_COL)))
        ).count();
        return multigraphViolations == 0;
    }
}
