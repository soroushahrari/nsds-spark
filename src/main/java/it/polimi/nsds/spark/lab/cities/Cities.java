package it.polimi.nsds.spark.lab.cities;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;


public class Cities {
    public static void main(String[] args) throws TimeoutException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> citiesRegionsFields = new ArrayList<>();
        citiesRegionsFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesRegionsFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        final StructType citiesRegionsSchema = DataTypes.createStructType(citiesRegionsFields);

        final List<StructField> citiesPopulationFields = new ArrayList<>();
        citiesPopulationFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        citiesPopulationFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesPopulationFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
        final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationFields);

        final Dataset<Row> citiesPopulation = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesPopulationSchema)
                .csv(filePath + "files/cities/cities_population.csv");

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "files/cities/cities_regions.csv");

        //Q1:

        final Dataset<Row> joinedTables = citiesPopulation
                .join(citiesRegions, "city");
        joinedTables.cache();

        final Dataset<Row> q1 = joinedTables
                .groupBy("region")
                .sum("population")
                .select("region", "sum(population)");

        q1.show();


        //Q2:

        final Dataset<Row> maxPopulationPerRegion = joinedTables
                .groupBy("region")
                .max("population")
                .select("region",  "max(population)");

        final Dataset<Row> q2 = joinedTables
                .groupBy("region")
                .agg(col("region"), count("city"))
                .select("region", "count(city)")
                .join(maxPopulationPerRegion, "region");

        q2.show();
        joinedTables.unpersist();

        //Q3:

        // JavaRDD where each element is an integer and represents the population of a city
        JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));
        JavaRDD<Integer> oldPopulation;

        population.cache();

        Integer counter = 1;
        Integer totalPopulation;

        while((totalPopulation = population.reduce(Integer::sum)) < 100000000){
            oldPopulation = population;
            population = modifyPopulation(population);

            oldPopulation.unpersist();
            population.cache();

            System.out.println("Year: " + counter + ", Total population: "+ totalPopulation);
            counter++;
        }

        //Q4:
        // Compute the total number of bookings for each region, in
        // a window of 30 seconds, sliding every 5 seconds

        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();

        final Dataset<Row> bookings_joined = bookings
                .join(joinedTables, joinedTables.col("id").equalTo(bookings.col("value")))
                .groupBy(window(col("timestamp"), "30 seconds", "5 seconds"), col("region"))
                .count();

        final StreamingQuery q4 = bookings_joined
                .writeStream()
                .outputMode("update1")
                .format("console")
                .start();

        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }


    // Q3 helper function
    public static JavaRDD<Integer> modifyPopulation(JavaRDD<Integer> population) {
        return population.map(r -> {
            if (r > 1000) {
                return (int)(r * 1.01);
            } else {
                return (int)(r * 0.99);
            }
        });
    }
}