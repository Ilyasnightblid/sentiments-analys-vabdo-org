package com.sentiments.service;

import com.sentiments.util.ModelLoader;
import javafx.concurrent.Task;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

public class SentimentPredictor {
    private static PipelineModel model = null;
    private static SparkSession sparkSession;

    public SentimentPredictor(String modelPath) {
        if (sparkSession == null) {
            sparkSession = SparkSession.builder()
                    .appName("Sentiment Analysis")
                    .master("local[*]") // Adjust master as needed
                    .getOrCreate();
        }

        if (model == null) {
            model = ModelLoader.getModel(modelPath);
        }
    }

    public Task<String> predictSentiment(String review) {
        return new Task<>() {
            @Override
            protected String call() throws Exception {
                Dataset<Row> inputData = sparkSession.createDataset(
                        java.util.Collections.singletonList(review),
                        Encoders.STRING()
                ).toDF("review");

                Dataset<Row> predictions = model.transform(inputData);
                Row prediction = predictions.select("prediction").first();
                double pred = prediction.getDouble(0);
                return pred == 1.0 ? "Positive" : "Negative";
            }
        };
    }

    public static void closeSpark() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
        ModelLoader.closeSpark();
    }
}
