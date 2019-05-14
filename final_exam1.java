// TEMPLATE FOR HADOOP MAPREDUCE
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang3.math.NumberUtils;
// For creating a job, setting the mapper and reducer, and setting the input and output paths
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Key will be 2
// Value will be 26

public class final_exam1{
    public static class final_exam1MAP extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            if (NumberUtils.isNumber(linevalues[25])){
                context.write(new Text(linevalues[2]), new Text(linevalues[25]));
            }
            else {
                context.write(new Text(linevalues[2]), new Text("0"));
            }
        }
    }

    public static class final_exam1REDUCE extends Reducer<Text, Text, Text, FloatWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Float average_price = 0.0f;
            int count = 0;
            for (Text val : values){
                average_price = average_price + Float.parseFloat(val.toString());
                count = count + 1;
            }
            average_price = average_price / count;
            context.write(new Text(key + ": "), new FloatWritable(average_price));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "final_exam1");

        // Jar name when running
        job.setJarByClass(final_exam1.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(final_exam1MAP.class);
        job.setReducerClass(final_exam1REDUCE.class);
        // Output of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Output of Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d final_exam1/ final_exam1.java
// jar -cvf final_exam1.jar -C final_exam1/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/final_exam1.jar 
// org.apache.hadoop.ramapo.final_exam1 ~/input/final_exam1/final_exam1.txt ~/hadoop_output/final_exam1
