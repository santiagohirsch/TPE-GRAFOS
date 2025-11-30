package ar.edu.itba.graph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import ar.edu.itba.graph.utils.SparkUtils;

import java.io.IOException;
import java.util.List;

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
            System.err.println("The underlying structure is not valid, it is a multigraph.");
            System.exit(1);
        }

        Dataset<Row> reversed = edgesDF.select(
                edgesDF.col(GRAPHFRAMES_DST_COL).alias(GRAPHFRAMES_SRC_COL),
                edgesDF.col(GRAPHFRAMES_SRC_COL).alias(GRAPHFRAMES_DST_COL)
        );
        edgesDF = edgesDF.union(reversed).distinct();

        GraphFrame graph = GraphFrame.apply(verticesDF, edgesDF);

        int k = Integer.parseInt(args[2]);

        GraphFrame result = kCore.decomposition(graph, k);

        sparkUtils.getSparkContext().close();
    }

    private GraphFrame decomposition(GraphFrame graph, int k) {
        boolean changed;
        GraphFrame currentGraph = graph;

        do {
            Dataset<Row> degrees = currentGraph.degrees();

            Dataset<Row> filteredVertices = currentGraph.vertices()
                    .join(degrees, currentGraph.vertices().col(GRAPHFRAMES_ID_COL).equalTo(degrees.col(GRAPHFRAMES_ID_COL)))
                    .filter(GRAPHFRAMES_DEGREE_COL + " >= " + k);

            Dataset<Row> filteredEdges = currentGraph.edges()
                    .join(filteredVertices, currentGraph.edges().col(GRAPHFRAMES_SRC_COL).equalTo(filteredVertices.col(GRAPHFRAMES_ID_COL)))
                    .join(filteredVertices, currentGraph.edges().col(GRAPHFRAMES_DST_COL).equalTo(filteredVertices.col(GRAPHFRAMES_ID_COL)))
                    .select(currentGraph.edges().col(GRAPHFRAMES_ALL_COLS));

            GraphFrame newGraph = GraphFrame.apply(filteredVertices, filteredEdges);

            changed = newGraph.vertices().count() != currentGraph.vertices().count();
            currentGraph = newGraph;
        } while (changed);

        List<Row> result = currentGraph.vertices().collectAsList();
        for (Row row : result) {
            System.out.println(row);
        }

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
