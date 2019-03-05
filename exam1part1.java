// TEMPLATE FOR HADOOP MAPREDUCE
package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.StringTokenizer;;
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

public class Exam1{
    public static class Exam1Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            for (int i = 0; i < linevalues.length; i++){
                context.write(new Text(linevalues[i]), new IntWritable(1));
            }
        }
    }

    public static class Exam1Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        private Text max_letter = new Text("");
        private int max_freq = 0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            Text letter = new Text(key);
            for (IntWritable val : values){
                sum += val.get();
                if (sum > max_freq){
                    max_freq = sum;
                    max_letter = letter;
                }
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(max_letter, new IntWritable(max_freq));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exam1");

        // Jar name when running
        job.setJarByClass(Exam1.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(Exam1Map.class);
        job.setReducerClass(Exam1Reduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Output of Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d Exam1/ Exam1.java
// jar -cvf Exam1.jar -C Exam1/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/Exam1.jar 
// org.apache.hadoop.ramapo.Exam1 ~/input/Exam1/Exam1.txt ~/hadoop_output/Exam1