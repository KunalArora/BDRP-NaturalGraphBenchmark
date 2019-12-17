import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.VertexRDD;
import java.io.FileNotFoundException;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.graphx.lib.PageRank;

import scala.Tuple2;

public class graphx_bench {

    public static void graphx_bench(JavaSparkContext ctx) throws FileNotFoundException {
        // Load the edges as a graph
        Graph<Object, Object> graph =  GraphLoader.edgeListFile(ctx.sc(), "./src/main/resources/facebook_combined.txt", true, 1, StorageLevel.MEMORY_AND_DISK_SER(), StorageLevel.MEMORY_AND_DISK_SER());

        EdgeRDD<Object> edge = graph.edges();
        VertexRDD<Object> vertices = graph.vertices();

        edge.toJavaRDD().foreach(new VoidFunction<Edge<Object>>() {
            public void call(Edge<Object> arg0) throws Exception {
                System.out.println("Edges: " + arg0.toString());
            }
        });
        vertices.toJavaRDD().foreach(new VoidFunction<Tuple2<Object,Object>>() {
            public void call(Tuple2<Object, Object> arg0) throws Exception {
                System.out.println("Vertices: " + arg0.toString());
            }
        });

        Graph<Object, Object> pageRank = PageRank.run(graph, 10, 0.0001, graph.vertices().vdTag(), graph.vertices().vdTag());

        EdgeRDD<Object> edgeRDD = pageRank.edges();
        VertexRDD<Object> vertexRDD = pageRank.vertices();

        edgeRDD.toJavaRDD().foreach(new VoidFunction<Edge<Object>>() {
            public void call(Edge<Object> arg0) throws Exception {
                System.out.println(arg0.toString());
            }
        });

        vertexRDD.toJavaRDD().foreach(new VoidFunction<Tuple2<Object,Object>>() {
            public void call(Tuple2<Object, Object> arg0) throws Exception {
                System.out.println(arg0.toString());
            }
        });

        JavaPairRDD<Long, Double> rank = vertexRDD.toJavaRDD().mapToPair(new PairFunction<Tuple2<Object,Object>,Long, Double>() {
            public Tuple2<Long, Double> call(Tuple2<Object, Object> arg0)
                    throws Exception {
                return new Tuple2<Long, Double>((Long)arg0._1(), (Double)arg0._2());
            }
        });

        rank.sortByKey().foreach(new VoidFunction<Tuple2<Long, Double>>() {
            @Override
            public void call(Tuple2<Long, Double> arg0) throws Exception {
                System.out.println(arg0.toString());
            }
        });

        // To see the sorting with respect to page ranks
//        JavaPairRDD<Double, Long> rank_rev = rank.mapToPair(t -> new Tuple2<Double, Long>(t._2, t._1));
//
//        rank_rev.sortByKey().foreach(new VoidFunction<Tuple2<Double, Long>>() {
//            @Override
//            public void call(Tuple2<Double, Long> arg0) throws Exception {
//                System.out.println(arg0.toString());
//            }
//        });


        // To rank the pages according to vertices.
//        JavaRDD<String> input = ctx.textFile("./src/main/resources/vert");
//
//        JavaPairRDD userRDD = input.mapToPair(new PairFunction<String, String>() {
//            public Tuple2<String, String> call(String arg0) throws Exception {
//                return new Tuple2(arg0.toString(), arg0.toString());
//            }
//        });
//
//        JavaPairRDD<String, String>  ranksByUsername  = userRDD.join(rank).mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,String>>, String, String>() {
//            public Tuple2<String, String> call(
//                    Tuple2<Long, Tuple2<String, String>> arg0) throws Exception {
//                return new Tuple2<String, String>(arg0._2()._1(),arg0._2()._2());
//            }
//        });
//
//        ranksByUsername.foreach(new VoidFunction<Tuple2<String,String>>() {
//            public void call(Tuple2<String, String> arg0) throws Exception {
//                System.out.println(arg0);
//            }
//        });
    }
}
