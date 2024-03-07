package lab1;

import com.github.sh0nk.matplotlib4j.Plot;
import com.github.sh0nk.matplotlib4j.PythonExecutionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class JoinMoviesRatings {
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
                .withColumnRenamed("title2", "title");

        Dataset<Row> df_ratings = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/ratings.csv");

        var df_mr = df_movies.join(df_ratings, "movieId", "inner");

        var df_mr_t = df_mr.groupBy("title").agg(
                        min("rating").as("min_rating"),
                        avg("rating").as("avg_rating"),
                        max("rating").as("max_rating"),
                        count("rating").as("rating_cnt"))
                .orderBy(desc("rating_cnt"));

//        df_mr_t.show();

        var avgRatings = df_mr_t.select("avg_rating").where("rating_cnt>=0").as(Encoders.DOUBLE()).collectAsList();

//        plot_histogram(avgRatings, "Average ratings histogram");

        var rating_cnt = df_mr_t.select("rating_cnt").where("avg_rating>=4.5").as(Encoders.DOUBLE()).collectAsList();

//        plot_histogram(rating_cnt, "Rating count histogram for movies with average rating >= 4.5");

        var complex_predicate = df_mr_t.select("rating_cnt")
                .where("avg_rating>=3.5 and rating_cnt>20")
                .as(Encoders.DOUBLE()).collectAsList();

//        plot_histogram(complex_predicate, "Rating count histogram for movies with average rating >= 3.5 and rating count > 20");


        var df_release = df_mr.withColumn("release_year", col("year").cast("int"))
                .withColumn("rating_year", year(from_unixtime(col("timestamp"))))
                .withColumn("release_to_rating_year", col("rating_year").minus(col("release_year")))
                .drop("release_year", "rating_year");

        df_release = df_release.filter("release_to_rating_year !=-1")
                .filter("release_to_rating_year IS NOT NULL");

        var release_year_diff = df_release.select("release_to_rating_year").where("release_to_rating_year>=0").sample(0.1).as(Encoders.DOUBLE()).collectAsList();

        plot_histogram(release_year_diff, "Release to rating year difference histogram");

        var df_plot_values = df_release.groupBy("release_to_rating_year")
                .count().as("count")
                .orderBy("release_to_rating_year");

        var release_year_diff2 = df_plot_values.select("release_to_rating_year").as(Encoders.DOUBLE()).collectAsList();
        var count = df_plot_values.select("count").as(Encoders.DOUBLE()).collectAsList();

        plot_histogram2(release_year_diff2, count, "Release to rating year difference histogram");
    }

    static void plot_histogram(List<Double> x, String title) {
        Plot plt = Plot.create();
        plt.hist().add(x).bins(50);
        plt.title(title);
        try {
            plt.show();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (PythonExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    static void plot_histogram2(List<Double> x, List<Double> weights, String title) {
        Plot plt = Plot.create();
        plt.hist().add(x).weights(weights).bins(50);
        plt.title(title);
        try {
            plt.show();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (PythonExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
