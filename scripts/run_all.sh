#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
PROJECT_DIR=$(dirname "$0")/..
DATA_RAW_DIR=$PROJECT_DIR/data/raw
DATA_PROCESSED_DIR=$PROJECT_DIR/data/processed
MODEL_OUTPUT_DIR=$PROJECT_DIR/data/models/sentiment_model
HDFS_RAW_PATH=/data/raw/imdb_reviews.csv
HDFS_PROCESSED_PATH=/data/processed/
HDFS_MODEL_PATH=/data/models/sentiment_model

# Ensure Hadoop and Spark are configured
echo "Verifying Hadoop and Spark configurations..."

# Start Hadoop and Spark services if not already running
echo "Starting Hadoop HDFS..."
start-dfs.sh
echo "Starting Hadoop YARN..."
start-yarn.sh
echo "Starting Spark services..."
start-all.sh

# Upload raw data to HDFS
echo "Uploading raw data to HDFS..."
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put -f $DATA_RAW_DIR/imdb_reviews.csv /data/raw/imdb_reviews.csv

# Build the project
echo "Building the project with Maven..."
cd $PROJECT_DIR
mvn clean package

# Run Data Preprocessing
echo "Running Data Preprocessing MapReduce job..."
hadoop jar target/sentiment-analysis-system-1.0-SNAPSHOT.jar com.sentiments.preprocessing.DataPreprocessing /data/raw /data/processed

# Run Model Training
echo "Running Model Training Spark job..."
spark-submit \
    --class com.sentiments.training.ModelTraining \
    --master yarn \
    --deploy-mode cluster \
    target/sentiment-analysis-system-training-1.0-SNAPSHOT-training.jar $HDFS_PROCESSED_PATH $HDFS_MODEL_PATH

# Download the trained model from HDFS to local models directory
echo "Downloading the trained model from HDFS..."
hdfs dfs -rm -rf $MODEL_OUTPUT_DIR
hdfs dfs -get $HDFS_MODEL_PATH $MODEL_OUTPUT_DIR

# Launch JavaFX Application
echo "Launching JavaFX Sentiment Analysis Application..."
mvn javafx:run




