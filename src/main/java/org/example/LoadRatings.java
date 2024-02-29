package org.example;

import com.github.sh0nk.matplotlib4j.NumpyUtils;
import com.github.sh0nk.matplotlib4j.Plot;
import com.github.sh0nk.matplotlib4j.PythonExecutionException;
import org.apache.spark.sql.*;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class LoadRatings {

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
                .load("src/main/resources/ratings.csv");

        var df_transformed = df.withColumn("datetime", functions.from_unixtime(df.col("timestamp")))
                .withColumn("year", functions.year(col("datetime")))
                .withColumn("month", functions.month(col("datetime")))
                .withColumn("day", functions.dayofmonth(col("datetime")));

        var df_stats_ym = df_transformed.groupBy(df_transformed.col("year"), df_transformed.col("month")).count().orderBy(df_transformed.col("year"), df_transformed.col("month"));
        df_stats_ym.show(1000);
        plot_stats_ym(df_stats_ym, "Ratings per month", "Ratings");
    }

    static void plot_stats_ym(Dataset<Row> df, String title, String label) {
        var labels = df.select(concat(col("year"), lit("-"), col("month"))).as(Encoders.STRING()).collectAsList();
        var x = NumpyUtils.arange(0, labels.size() - 1, 1);
        x = df.select(expr("year+(month-1)/12")).as(Encoders.DOUBLE()).collectAsList();
        var y = df.select("count").as(Encoders.DOUBLE()).collectAsList();
        Plot plt = Plot.create();
        plt.plot().add(x, y).linestyle("-").label(label);
        plt.legend();
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
