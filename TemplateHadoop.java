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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
// For creating a job, setting the mapper and reducer, and setting the input and output paths
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FILENAME{
    public static class FILENAMEMAP extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
        }
    }

    public static class FILENAME extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FILENAME");

        // Jar name when running
        job.setJarByClass(FILENAME.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(FILENAMEMAP.class);
        job.setReducerClass(FILENAMEREDUCE.class);
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

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d FILENAME/ FILENAME.java
// jar -cvf FILENAME.jar -C FILENAME/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/FILENAME.jar 
// org.apache.hadoop.ramapo.FILENAME ~/input/FILENAME/FILENAME.txt ~/hadoop_output/FILENAME