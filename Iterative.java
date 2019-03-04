// Hadoop has built-in functionality for sorting since this is something
// that hadoop has difficulty with normall.
// Sorting happens between the mapping and the reducing.
// This program will implement this functionality.
// 

package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.util.StringTokenizer;
// Configuration and path requirements
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// Hadoop datatypes
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
// For creating a job, setting the mapper and reducer, and setting the input and output paths
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Iterative{
    public static class IterativeMap extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            // Since we want to sort all of the values, we are passing the values from the file
            // as the key to the recducer. This means that the output of the reducer will be a 
            // sorted list of all of the keys from the mapper.
            context.write(new IntWritable(Integer.parseInt(linevalues[1])), 
                new IntWritable(Integer.parseInt(linevalues[0])));
        }
    }

    public static class IterativeReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            // Need to check for repitition of values
            for(IntWritable val : values){
                context.write(key, new Text());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Iterative");

        // Jar name when running
        job.setJarByClass(Iterative.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(IterativeMap.class);
        job.setReducerClass(IterativeReduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Output of Reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d Iterative/ Iterative.java
// jar -cvf Iterative.jar -C Iterative/ .

// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/Iterative.jar org.apache.hadoop.ramapo.Iterative 
// ~/input/Iterative/Iterative.txt ~/hadoop_output/Iterative