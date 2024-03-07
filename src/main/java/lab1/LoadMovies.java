package lab1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class LoadMovies {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("LoadUsers")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/movies.csv");

        var df2 = df
                .withColumn("rok", year(now()))
                .withColumn("miesiac", month(now()))
                .withColumn("dzien", day(now()))
                .withColumn("godzina", hour(now()));

        var df3 = df
                .withColumn("title2", regexp_extract(col("title"), "^(.*?)\\s*\\((\\d{4})\\)$", 1))
                .withColumn("year", regexp_extract(col("title"), "^(.*?)\\s*\\((\\d{4})\\)$", 2))
                .drop("title")
                .withColumnRenamed("title2", "title")
                .withColumn("genre", split(col("genres"), "\\|"))
                .drop("genres");

        // get genreList from df3 - explode and use distinct select
        var genreList = df3.select(explode(col("genre"))).distinct().as(Encoders.STRING()).collectAsList();
        for (var s : genreList) {
            System.out.println(s);
        }
        var df_multigenre = df3;
        for (var s : genreList) {
            if (s.equals("(no genres listed)")) continue;
            df_multigenre = df_multigenre.withColumn(s, array_contains(col("genre"), s));
        }
        df_multigenre = df_multigenre.drop("genre");
        df_multigenre.show();
    }


}
