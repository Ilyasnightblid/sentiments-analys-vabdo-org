package com.sentiments.util;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.SparkSession;

public class ModelLoader {
    private static PipelineModel model = null;
    private static SparkSession spark = null;

    public static PipelineModel getModel(String modelPath) {
        if (model == null) {
            spark = SparkSession.builder()
                    .appName("Sentiment Analysis Model Loader")
                    .master("local[*]") // Adjust as needed
                    .getOrCreate();
            model = PipelineModel.load(modelPath);
        }
        return model;
    }

    public static void closeSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
}
