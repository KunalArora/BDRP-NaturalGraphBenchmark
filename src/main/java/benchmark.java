import com.google.common.collect.Lists;
import org.apache.html.dom.HTMLInputElementImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import org.graphframes.GraphFrame;

import java.io.*;
import java.util.ArrayList;
import java.io.IOException;
import java.util.List;

public class benchmark {

    public static void benchma(JavaSparkContext ctx, SQLContext sqlCtx) throws FileNotFoundException {
        long starttime_vertex = System.nanoTime();
        // vertex creation
        List<Row> vertices_list = new ArrayList<Row>();
        int i =0 ;
        try (BufferedReader br = new BufferedReader(new FileReader("./src/main/resources/vert"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String id = line;
//                String id = id_name[0];
//                String name = id_name[1];
                vertices_list.add(RowFactory.create(id));
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Read "+i+" vertex lines");
        JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
        StructType vertices_schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
        });
        Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
        long endtime_vertex   = System.nanoTime();
        long totaltime_vertex = endtime_vertex - starttime_vertex;
        System.out.println("Vertex creation time" + totaltime_vertex);

        long starttime_edge = System.nanoTime();
        // edges creation
        List<Row> edges_list = new ArrayList<Row>();
        i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("./src/main/resources/edges.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] id1_id2 = line.split(",", 2);
                String id1 = id1_id2[0];
                String id2 = id1_id2[1];
                edges_list.add(RowFactory.create(id1, id2));
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Read "+i+" edge lines");
        JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
        StructType edges_schema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
        });
        Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
        long endtime_edge= System.nanoTime();
        long totaltime_edge = endtime_edge - starttime_edge;
        System.out.println("Edge creation time" + totaltime_edge);

        long start = System.nanoTime();
        long starttime_pagerank = System.nanoTime();
        int max_iter = 1;
        float df = (float) 0.1;

        GraphFrame gf = GraphFrame.apply(vertices,edges);
          Dataset<Row> res = gf.labelPropagation().maxIter(1).run();
//        GraphFrame result = gf.labelPropagation.maxIter(5).run();
//        ((Dataset) res).select("")
//        res.select("id", "label").show();
        long endtime_pagerank = System.nanoTime();
        long totaltime_pagerank = endtime_pagerank - starttime_pagerank;
        System.out.println("Total time page rank" + totaltime_pagerank);

    }
}
