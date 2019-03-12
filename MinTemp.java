// Cleanup for a reducer
package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Map;
import java.util.TreeMap;;
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

public class MinTemp {

    public static class MinTempMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] linevalues = value.toString().split(",");
            context.write(new Text(), new Text(linevalues[0] + "," + linevalues[1] + "," + linevalues[2]));
        }
    }

    public static class MinTempReduce extends Reducer<Text, Text, Text, Text> {
        private Integer min_temp = 99999;
        private String min_month = "";
        private String min_city = "";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer current_temp = 0;
            for (Text val : values){
                String [] linevalues = val.toString().split(",");
                current_temp = Integer.parseInt(linevalues[2]);
                if (current_temp < min_temp){
                    min_temp = current_temp;
                    min_month = linevalues[1];
                    min_city = linevalues[0];
                }
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(min_city), new Text(min_month + "," + min_temp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MinTemp");

        // Jar name when running
        job.setJarByClass(MinTemp.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(MinTempMap.class);
        job.setReducerClass(MinTempReduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Output of Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// ------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d MinTemp/ MinTemp.java
// jar -cvf MinTemp.jar -C MinTemp/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/MinTemp.jar org.apache.hadoop.ramapo.MinTemp ~/input/MinTemp/MinTemp.txt ~/hadoop_output/MinTemp