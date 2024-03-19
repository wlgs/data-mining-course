package lab1;

import com.github.sh0nk.matplotlib4j.Plot;
import com.github.sh0nk.matplotlib4j.PythonExecutionException;
import org.apache.spark.sql.*;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class JoinUsersRatings {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("LoadUsers")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());


        Dataset<Row> df_users = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/users.csv");


        Dataset<Row> df_ratings = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/ratings.csv");

        var df_ur = df_users.join(df_ratings, "userId", "inner");

        var df_ur_t = df_ur.groupBy("email").agg(
                        avg("rating").as("avg_rating"),
                        count("rating").as("rating_cnt"))
                .orderBy(desc("rating_cnt"));
        df_ur_t.show();

        plot_stats_ym(df_ur_t, "Ratings per user", "rating_cnt");
    }

    static void plot_stats_ym(Dataset<Row> df, String title, String label) {
        var y = df.select(label).as(Encoders.DOUBLE()).collectAsList();
        var x = df.select("avg_rating").as(Encoders.DOUBLE()).collectAsList();
        Plot plt = Plot.create();
        plt.plot().add(x, y, "o").label("data");
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
