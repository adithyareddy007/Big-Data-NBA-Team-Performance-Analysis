package com.hadoop.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlayerAvgPPG {

    public static class PPGMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int PLAYER_INDEX = 2;
        private static final int GAMES_INDEX = 7;
        private static final int POINTS_INDEX = 29;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length <= POINTS_INDEX || fields[0].equalsIgnoreCase("Year")) return;

            String player = fields[PLAYER_INDEX].trim();
            String pointsStr = fields[POINTS_INDEX].trim();
            String gamesStr = fields[GAMES_INDEX].trim();

            if (!player.isEmpty() && !pointsStr.isEmpty() && !gamesStr.isEmpty()) {
                context.write(new Text(player), new Text(pointsStr + "," + gamesStr));
            }
        }
    }

    public static class PPGReducer extends Reducer<Text, Text, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalPoints = 0;
            int totalGames = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                try {
                    int points = Integer.parseInt(parts[0]);
                    int games = Integer.parseInt(parts[1]);
                    totalPoints += points;
                    totalGames += games;
                } catch (NumberFormatException e) {
                    continue; // skip invalid lines
                }
            }

            if (totalGames > 0) {
                float ppg = (float) totalPoints / totalGames;
                context.write(key, new FloatWritable(ppg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PlayerAvgPPG <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Points Per Game");

        job.setJarByClass(PlayerAvgPPG.class);
        job.setMapperClass(PPGMapper.class);
        job.setReducerClass(PPGReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
