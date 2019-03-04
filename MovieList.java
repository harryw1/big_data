package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.util.StringTokenizer;

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

public class MovieList{
    public static class MovieListMap extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String [] linevalues = line.toString().split(",");
        
            context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(linevalues[1]+","+linevalues[2]));
        }
    }

    public static class MovieListReducer extends Reducer<IntWritable, Text, NullWritable, IntWritable>{
        private float max_rating;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String [] linevalues;
            Integer movie_id = 0;
            float rating;

            for(Text val:values){
                linevalues = val.toString().split(",");
                rating = Float.parseFloat(linevalues[1]);
                
                if (rating > max_rating){
                    max_rating = rating;
                    movie_id = Integer.parseInt(linevalues[0]);
                }
            }
            context.write(NullWritable.get(), new IntWritable(movie_id));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie List");

        job.setJarByClass(MovieList.class);
        job.setMapperClass(MovieListMap.class);
        job.setReducerClass(MovieListReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d <FILENAME>/ <FILENAME>.java
// jar -cvf <FILENAME>.jar -C <FILENAME>/ .

// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/<FILENAME>.jar org.apache.hadoop.ramapo.<FILENAME> ~/input/<FILENAME>.txt ~/<OUTPUTFOLDER>