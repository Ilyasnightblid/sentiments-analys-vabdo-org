package com.sentiments.ui;

import com.sentiments.service.SentimentPredictor;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;

import java.io.InputStream;
import java.util.Properties;

public class MainController {

    @FXML
    private TextArea reviewTextArea;

    @FXML
    private Button predictButton;

    @FXML
    private Label resultLabel;

    private SentimentPredictor predictor;

    @FXML
    public void initialize() {
        // Load application properties
        Properties properties = new Properties();
        try (InputStream input = getClass().getResourceAsStream("/application.properties")) {
            properties.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String modelPath = properties.getProperty("model.path", "data/models/sentiment_model");
        predictor = new SentimentPredictor(modelPath);
    }

    @FXML
    public void handlePredict() {
        String review = reviewTextArea.getText().trim();
        if (review.isEmpty()) {
            resultLabel.setText("Sentiment: Please enter a review.");
            return;
        }

        Task<String> predictionTask = predictor.predictSentiment(review);

        predictionTask.setOnSucceeded(event -> {
            String sentiment = predictionTask.getValue();
            resultLabel.setText("Sentiment: " + sentiment);
        });

        predictionTask.setOnFailed(event -> {
            resultLabel.setText("Sentiment: Error during prediction.");
        });

        new Thread(predictionTask).start();
    }
}
