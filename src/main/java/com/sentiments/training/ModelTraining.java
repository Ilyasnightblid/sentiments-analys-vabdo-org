package com.sentiments.training;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.IOException;

public class ModelTraining {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: ModelTraining <input path> <model output path>");
            System.exit(1);
        }

        String inputPath = args[0]; // e.g., hdfs://localhost:9000/data/processed/
        String modelOutputPath = args[1]; // e.g., /data/models/sentiment_model

        SparkSession spark = SparkSession.builder()
                .appName("Sentiment Analysis Model Training")
                .getOrCreate();

        // Load the data
        Dataset<Row> data = spark.read().option("header", "false")
                .csv(inputPath)
                .toDF("review", "sentiment");

        // Convert sentiment labels to numerical values
        Dataset<Row> labeledData = data.withColumn("label",
                        functions.when(functions.col("sentiment").equalTo("positive"), 1.0)
                                .otherwise(0.0))
                .select("review", "label");

        // Data preprocessing
        Tokenizer tokenizer = new Tokenizer().setInputCol("review").setOutputCol("words");
        StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
        HashingTF hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("features").setNumFeatures(10000);
        LogisticRegression lr = new LogisticRegression().setMaxIter(20).setRegParam(0.001);

        // Build the pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, remover, hashingTF, lr});

        // Train the model
        PipelineModel model = pipeline.fit(labeledData);

        // Save the model
        model.write().overwrite().save(modelOutputPath);

        spark.stop();
    }
}
