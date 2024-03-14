package lab3;

import com.github.sh0nk.matplotlib4j.NumpyUtils;
import com.github.sh0nk.matplotlib4j.Plot;
import com.github.sh0nk.matplotlib4j.PythonExecutionException;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class Xy002 {

    static double realFunction(double x) {
        return -1.5*(x*x) + 3*x +4;
    };
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
                .load("src/main/resources/lab3/xy-002.csv");

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[] {"X"})
                .setOutputCol("features");
        var df_trans = vectorAssembler.transform(df);


        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFeaturesCol("features")
                .setLabelCol("Y");

        LinearRegressionModel lrModel = lr.fit(df_trans);

        System.out.println("Parameters" + lrModel.coefficients() + " and " + lrModel.intercept());
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show(100);
        System.out.println("MSE: " + trainingSummary.meanSquaredError());
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("MAE: " + trainingSummary.meanAbsoluteError());
        System.out.println("r2: " + trainingSummary.r2());

        List<Double> lossHistory = Arrays.stream(trainingSummary.objectiveHistory()).boxed().toList();
        plotObjectiveHistory(lossHistory);


        var x = df_trans.select("X").as(Encoders.DOUBLE()).collectAsList().stream().toList();
        var y = df_trans.select("Y").as(Encoders.DOUBLE()).collectAsList().stream().toList();

        plot(x, y, lrModel, "Linear Regression xy002", Xy002::realFunction);
    }

    static void plotObjectiveHistory(List<Double> lossHistory){
        var x = IntStream.range(0,lossHistory.size()).mapToDouble(d->d).boxed().toList();
        Plot plt = Plot.create();
        plt.plot().add(x, lossHistory).label("loss");
        plt.xlabel("Iteration");
        plt.ylabel("Loss");
        plt.title("Loss history");
        plt.legend();
        try {
            plt.show();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (PythonExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param x - x axis data
     * @param y - y axis data
     * @param lrModel - regression model
     * @param title - title for plot
     * @param f_true - function representing true model (can be null)
     */
    static void plot(List<Double> x, List<Double> y, LinearRegressionModel lrModel, String title, Function<Double,Double> f_true){
        var x_ = x.stream().mapToDouble(d->d).toArray();
        var y_ = y.stream().mapToDouble(d->d).toArray();
        var x_min = Arrays.stream(x_).min().getAsDouble();
        var x_max = Arrays.stream(x_).max().getAsDouble();
        var xdelta = 0.05*(x_min-x_max);
        var x__ = NumpyUtils.linspace(x_min-xdelta,x_max+xdelta,100);
        var y__ = x__.stream().map(val -> lrModel.predict(Vectors.dense(val))).mapToDouble(d->d).boxed().toList();
        Plot plt = Plot.create();
        plt.plot().add(x, y, "o").label("data");
        plt.plot().add(x__, y__).color("r").label("pred");
        if(f_true!=null){
            var y_true = x.stream().map(f_true).mapToDouble(d->d).boxed().toList();
            plt.plot().add(x, y_true).color("g").linestyle("--").label("$f_{true}$");
        }
        plt.xlabel("X");
        plt.ylabel("Y");
        plt.title(title);
        plt.legend();
        try {
            plt.show();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (PythonExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}