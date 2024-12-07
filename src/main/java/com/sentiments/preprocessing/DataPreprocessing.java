package com.sentiments.preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;

public class DataPreprocessing extends Configured implements Tool {
    private static final String COUNTER_GROUP = "Preprocessing Metrics";
    private static final Pattern CLEAN_PATTERN = Pattern.compile("[^a-zA-Z\\s]");

    public static class PreprocessingMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text reviewText = new Text();
        private final Text sentiment = new Text();
        private static final Pattern SPLIT_PATTERN =
                Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        @Override
        protected void setup(Context context) {
            // Initialize any resources if needed
            context.getCounter(COUNTER_GROUP, "Mapper Tasks").increment(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException {
            try {
                String line = value.toString();
                if (line.startsWith("review") || line.isEmpty()) {
                    context.getCounter(COUNTER_GROUP, "Skipped Headers/Empty").increment(1);
                    return;
                }

                String[] fields = SPLIT_PATTERN.split(line, -1);
                if (fields.length == 2) {
                    String review = cleanReview(fields[0]);
                    String sentimentLabel = cleanSentiment(fields[1]);

                    if (isValidReview(review, sentimentLabel)) {
                        reviewText.set(review);
                        sentiment.set(sentimentLabel);
                        context.write(reviewText, sentiment);
                        context.getCounter(COUNTER_GROUP, "Processed Reviews").increment(1);
                    } else {
                        context.getCounter(COUNTER_GROUP, "Invalid Reviews").increment(1);
                    }
                } else {
                    context.getCounter(COUNTER_GROUP, "Malformed Lines").increment(1);
                }
            } catch (Exception e) {
                context.getCounter(COUNTER_GROUP, "Processing Errors").increment(1);
                throw new IOException("Error processing line: " + value, e);
            }
        }

        private String cleanReview(String review) {
            return CLEAN_PATTERN
                    .matcher(review.replaceAll("\"", "").trim())
                    .replaceAll(" ")
                    .toLowerCase()
                    .replaceAll("\\s+", " ")
                    .trim();
        }

        private String cleanSentiment(String sentiment) {
            return sentiment.replaceAll("\"", "").trim().toLowerCase();
        }

        private boolean isValidReview(String review, String sentiment) {
            return !review.isEmpty() &&
                    (sentiment.equals("positive") || sentiment.equals("negative"));
        }

        @Override
        protected void cleanup(Context context) {
            // Cleanup any resources if needed
        }
    }

    public static class PreprocessingReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) {
            context.getCounter(COUNTER_GROUP, "Reducer Tasks").increment(1);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Emit unique review-sentiment pairs
            String lastSentiment = null;
            for (Text val : values) {
                String currentSentiment = val.toString();
                if (lastSentiment == null || !lastSentiment.equals(currentSentiment)) {
                    context.write(key, val);
                    context.getCounter(COUNTER_GROUP, "Unique Reviews").increment(1);
                    lastSentiment = currentSentiment;
                } else {
                    context.getCounter(COUNTER_GROUP, "Duplicate Reviews").increment(1);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataPreprocessing <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        // Set optimal configuration parameters
        conf.setInt("mapreduce.job.maps", 10); // Adjust based on your cluster
        conf.setInt("mapreduce.job.reduces", 5);
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "IMDB Review Preprocessing");
        job.setJarByClass(getClass());

        // Set input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set job properties
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PreprocessingMapper.class);
        job.setReducerClass(PreprocessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add progress monitoring
        Thread monitorThread = new Thread(() -> {
            try {
                while (!job.isComplete()) {
                    System.out.printf("\rMap Progress: %.2f%%, Reduce Progress: %.2f%%",
                            job.mapProgress() * 100,
                            job.reduceProgress() * 100);
                    Thread.sleep(2000);
                }
                System.out.println("\nJob completed!");
            } catch (Exception e) {
                System.err.println("Error in monitoring thread: " + e.getMessage());
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DataPreprocessing(), args);
        System.exit(exitCode);
    }
}