package lab1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class JoinMoviesRatingsGenres {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("LoadUsers")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());


        Dataset<Row> df_movies = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/movies.csv");


        df_movies = df_movies.withColumn("year", regexp_extract(col("title"), "^(.*?)\\s*\\((\\d{4})\\)$", 2))
                .withColumn("title2",
                        when(regexp_extract(col("title"),"^(.*?)\\s*\\((\\d{4})\\)\\s*$",1).equalTo("")
                                ,col("title"))
                                .otherwise(regexp_extract(col("title"),"^(.*?)\\s*\\((\\d{4})\\)\\s*$",1)))
                .withColumn("year", regexp_extract(col("title"), "^(.*?)\\s*\\((\\d{4})\\)$", 2))
                .drop("title")
                .withColumnRenamed("title2", "title")
                .withColumn("genre", split(col("genres"), "\\|"))
                .drop("genres")
                .withColumn("genre", explode(col("genre")));



        Dataset<Row> df_ratings = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/ratings.csv");

        var df_mr = df_movies.join(df_ratings, "movieId", "inner");

        var df_mr_t = df_mr.groupBy("genre").agg(
                        min("rating").as("min_rating"),
                        avg("rating").as("avg_rating"),
                        max("rating").as("max_rating"),
                        count("rating").as("rating_cnt"))
                .orderBy(desc("rating_cnt"));
        df_mr_t.show();

        // select top3 genres based on avg_rating
        df_mr_t.select("*").orderBy(desc("avg_rating")).show(3);
        df_mr_t.select("*").orderBy(desc("rating_cnt")).show(3);

        // select top3 genres based on rating_cnt

        // filter those rows where avg_rating is higher than avg_rating of all df_ratings
        var avg_rating = df_ratings.agg(avg("rating")).as("avg_rating").collectAsList().get(0).getDouble(0);
        System.out.println("avg_rating: " + avg_rating);
        df_mr_t.filter(col("avg_rating").gt(avg_rating)).show();



    }
}
