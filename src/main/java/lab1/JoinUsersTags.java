package lab1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinUsersTags {
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

        Dataset<Row> df_tags = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("src/main/resources/tags.csv");

        df_users.createOrReplaceTempView("users");
        df_tags.createOrReplaceTempView("tags");

        String query = "SELECT email, tags FROM users JOIN tags ON users.userId = tags.userId GROUP BY users.email, users.userId";

        Dataset<Row> df_ut = spark.sql(query);

        df_ut.show();

        var tagsList = df_ut.select("tag").distinct().collectAsList();
        System.out.println("Unique tags:" + tagsList.size());

    }
}
