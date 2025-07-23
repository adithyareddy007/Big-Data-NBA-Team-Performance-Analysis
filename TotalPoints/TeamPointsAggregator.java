package com.hadoop.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop MapReduce job to aggregate total points by team from a CSV file.
 * Expects input CSV with team name in column 5 (index 5) and points in column 29 (index 29).
 */
public class TeamPointsAggregator {

    // Mapper Class
    public static class TeamPointsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int TEAM_COLUMN = 5;  // 'Tm' column index
        private static final int POINTS_COLUMN = 29; // 'PTS' column index

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                context.getCounter("Mapper Errors", "Empty Line").increment(1);
                return;
            }

            String[] fields = line.split(",");
            try {
                // Skip header row or invalid rows
                if (fields.length <= POINTS_COLUMN || fields[0].equalsIgnoreCase("Year")) {
                    context.getCounter("Mapper Errors", "Invalid Row or Header").increment(1);
                    return;
                }

                String team = fields[TEAM_COLUMN].trim();
                if (team.isEmpty()) {
                    context.getCounter("Mapper Errors", "Empty Team Name").increment(1);
                    return;
                }

                String pointsStr = fields[POINTS_COLUMN].trim();
                if (pointsStr.isEmpty()) {
                    context.getCounter("Mapper Errors", "Empty Points Value").increment(1);
                    return;
                }

                int points = Integer.parseInt(pointsStr);
                context.write(new Text(team), new IntWritable(points));
            } catch (NumberFormatException e) {
                context.getCounter("Mapper Errors", "Invalid Points Format").increment(1);
                System.err.println("Invalid points format in line: " + line);
            } catch (ArrayIndexOutOfBoundsException e) {
                context.getCounter("Mapper Errors", "Invalid Column Count").increment(1);
                System.err.println("Invalid column count in line: " + line);
            }
        }
    }

    // Reducer Class (also used as Combiner)
    public static class TeamPointsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalPoints = 0;
            for (IntWritable val : values) {
                totalPoints += val.get();
            }
            context.write(key, new IntWritable(totalPoints));
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        // Validate input arguments
        if (args.length != 2) {
            System.err.println("Usage: TeamPointsAggregator <input path> <output path>");
            System.exit(-1);
        }

        // Initialize Hadoop configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Team Total Points Aggregation");

        // Set JAR and class configurations
        job.setJarByClass(TeamPointsAggregator.class);
        job.setMapperClass(TeamPointsMapper.class);
        job.setCombinerClass(TeamPointsReducer.class);
        job.setReducerClass(TeamPointsReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit with appropriate status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}