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

        String query = "SELECT users.email, tags.tag FROM users JOIN tags ON users.userId = tags.userId";

        Dataset<Row> df_ut = spark.sql(query);

        df_ut.show();

        df_ut.createOrReplaceTempView("ut");
        String query2 = "SELECT email, concat_ws(' ', collect_list(tag)) as tags FROM ut GROUP BY email";
        Dataset<Row> df_ut2 = spark.sql(query2);
        df_ut2.show();

        // print all tags in console
        df_ut2.collectAsList().forEach(row -> {
            System.out.println(row.get(1));
        });
    }
}
