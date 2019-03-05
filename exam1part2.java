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

public class Exam1Part2{
    public static class Exam1Part2Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            if ((Integer.parseInt(linevalues[10])) == 1 && (Integer.parseInt(linevalues[24]) == 1)){
                context.write(new Text(linevalues[0]), new IntWritable());
            }
        }
    }

    public static class Exam1Part2Reduce extends Reducer<Text, IntWritable, Text, Text>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            for (IntWritable val : values){
                context.write(key, new Text("- Flag contains red and a triangle."));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exam1Part2");

        // Jar name when running
        job.setJarByClass(Exam1Part2.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(Exam1Part2Map.class);
        job.setReducerClass(Exam1Part2Reduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
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
// javac -classpath ${HADOOP_CLASSPATH} -d Exam1Part2/ Exam1Part2.java
// jar -cvf Exam1Part2.jar -C Exam1Part2/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/Exam1Part2.jar 
// org.apache.hadoop.ramapo.Exam1Part2 ~/input/Exam1Part2/Exam1Part2.txt ~/hadoop_output/Exam1Part2