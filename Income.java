// Income.java will sort by age group and report the number of individuals with
// an income greater than 50K
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

public class Income{
    public static class IncomeMap extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            String [] line = value.toString().split(",");
            String total_line = "";
            for (int i = 1; i < 15; i++){
                total_line += line[i]+",";
            }
            context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(total_line));
        }
    }

    public static class IncomeReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text val:values){
                context.write(key, new Text(val));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Income");

        // Jar name when running
        job.setJarByClass(Income.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(IncomeMap.class);
        job.setReducerClass(IncomeReduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
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
// javac -classpath ${HADOOP_CLASSPATH} -d Income/ Income.java
// jar -cvf Income.jar -C Income/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/Income.jar 
// org.apache.hadoop.ramapo.Income ~/input/Income/Income.txt ~/hadoop_output/Income