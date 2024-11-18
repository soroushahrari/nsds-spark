package it.polimi.nsds.spark.lab.friends;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

/**
 * Implement an iterative algorithm that implements the transitive closure of friends
 * (people that are friends of friends of ... of my friends).
 *
 * Set the value of the flag useCache to see the effects of caching.
 */
public class FriendsComputation {
    private static final boolean useCache = true;

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "FriendsCache" : "FriendsNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("person", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        final StructType schema = DataTypes.createStructType(fields);

        final Dataset<Row> input = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + "files/friends/friends.csv");

        if (useCache) {
            input.cache();
        }


        Dataset<Row> friends = input;
        Dataset<Row> result = input;

        while (result.count() > 0) {
            result = result.as("result").join(input.as("input"),
                        col("result.friend").equalTo(col("input.person")))
                    .select("result.person", "input.friend")
                    .withColumnRenamed("result.person", "person")
                    .withColumnRenamed("input.friend", "friend");
            
            friends = friends
                    .union(result)
                    .distinct();
            
            if(useCache) {
                result.cache();
            }

            friends.show();
        }

        // Dataset<Row> reduced = df.groupBy("key_column")
        //        .agg(collect_list("value_column").as("values"))

        //Dataset<Row> reduced = friends.groupBy("person").agg(collect_list("friend"));
        //reduced.show();

        /*



        Dataset<Row> distinctPeople = input.select("person")
                .union(input
                        .select("friend")
                        .withColumnRenamed("friend","person"))
                .distinct();

        

        distinctPeople.toJavaRDD().map(Row::toString).collect().mapToPair(x -> {

            return getFriendsOf(x.toString(), input, spark.sparkContext());
        });



    }

    public static JavaPairRDD<String, List<String>> getFriendsOf(String currentPerson, Dataset<Row> input, JavaSparkContext context){
        List<String> friendsOfCurrentPerson = input.where(col("person").equalTo(currentPerson))
                .select("friend")
                .as(Encoders.STRING())
                .collectAsList();
        return context.parallelize(List.of(new Tuple2<String, List<String>>(currentPerson, friendsOfCurrentPerson))).mapToPair(tuple -> tuple);
//        return new JavaPairRDD<String, List<String>> (currentPerson, friendsOfCurrentPerson);
    }

         */

        spark.close();
    }
}
