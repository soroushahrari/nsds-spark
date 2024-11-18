package it.polimi.nsds.spark.lab.enrichment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * This code snippet exemplifies a typical scenario in event processing: merging
 * incoming events with some background knowledge.
 *
 * A static dataset (read from a file) classifies products (associates products to the
 * class they belong to).  A stream of products is received from a socket.
 *
 * We want to count the number of products of each class in the stream. To do so, we
 * need to integrate static knowledge (product classification) and streaming data
 * (occurrences of products in the stream).
 */
public class EventEnrichment {
    public static void main(String[] args) throws TimeoutException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String socketHost = "localhost";
        final int socketPort = 9999;
//        final String filePath = args.length > 3 ? args[3] : "./";

        System.out.println("hi hi hi ");

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("EventEnrichment")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> productClassificationFields = new ArrayList<>();
        productClassificationFields.add(DataTypes.createStructField("product", DataTypes.StringType, false));
        productClassificationFields.add(DataTypes.createStructField("classification", DataTypes.StringType, false));
        final StructType productClassificationSchema = DataTypes.createStructType(productClassificationFields);

        final Dataset<Row> inStream = spark
                .readStream()
                .format("socket")
                .option("host", socketHost)
                .option("port", socketPort)
                //.option("rowsPerSecond", 1)
                .load()
                .withColumn("timestamp", current_timestamp());

        final Dataset<Row> productsClassification = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(productClassificationSchema)
                .csv("/home/soroush/nsds/NSDS_spark_lab_solutions/NSDS_spark_lab_solutions/files/enrichment/product_classification.csv");

        final Dataset<Row> result = inStream
                .join(productsClassification, "product")
                .groupBy(col("classification"),
                        window(col("timestamp"), "30 seconds", "10 seconds"))
                .count();

        final StreamingQuery query = result
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}
