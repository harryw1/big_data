package org.apache.hadoop.ramapo;

import java.io.IOException;
import java.util.TreeMap;
import java.util.StringTokenizer;
import java.util.Vector;
import java.lang.*;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopOneMovie {

  public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    private IntWritable userId = new IntWritable(1);
    private Text movieIdrating = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer it = new StringTokenizer(value.toString(), ",");
	    while(it.hasMoreTokens()){
        it.nextToken();
        movieIdrating.set(it.nextToken() + " " + it.nextToken());
        context.write(userId, movieIdrating);
      }
    }
  }

  public static class TenMoviesReducer extends Reducer<IntWritable, Text, IntWritable, NullWritable> {
    private IntWritable movieId = new IntWritable();
    float rating;
    String movie;
    float maxrating = Float.MIN_VALUE;
    String maxmovie = new String();
    public void reduce(IntWritable key, Iterable<Text> movieIdratings, Context context) throws IOException, InterruptedException {
      for(Text movieIdrating : movieIdratings){
        StringTokenizer it = new StringTokenizer(movieIdrating.toString());
          while(it.hasMoreTokens()){
            movie = it.nextToken();
            rating = Float.parseFloat(it.nextToken());
          }
          if(rating > maxrating){
            maxrating = rating;
            maxmovie = movie;
          }
        }
        movieId.set(Integer.parseInt(maxmovie));
        context.write(movieId, NullWritable.get());
      }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopOneMovie");
        job.setJarByClass(TopOneMovie.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(TenMoviesReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    }
