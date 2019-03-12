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

public class MaxTempRedo {

    public static class MaxTempRedoMap extends Mapper<LongWritable, Text, Text, Text> {
        // private TreeMap<Integer, Text> treemap = new TreeMap<Integer, Text>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] linevalues = value.toString().split(",");
            // treemap.put(Integer.parseInt(linevalues[1]), new Text(linevalues[0]));
            context.write(new Text(linevalues[0]), new Text(value));
        }

        // protected void cleanup(Context context) throws IOException, InterruptedException {
        //     for (Map.Entry<Integer, Text> entry : treemap.entrySet()) {
        //         context.write(new IntWritable(entry.getKey()), entry.getValue());
        //     }
        // }
    }

    public static class MaxTempRedoReduce extends Reducer<Text, Text, Text, IntWritable> {
        private Integer max_temp = 0;
        private String max_month = "";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer current_temp = 0;
            for (Text val : values){
                String [] linevalues = val.toString().split(",");
                current_temp = Integer.parseInt(linevalues[1]);
                if (current_temp > max_temp){
                    max_temp = current_temp;
                    max_month = linevalues[0];
                }
            }
            // for (Text val : values) {
            //     context.write(key, new Text(val));
            // }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(max_month), new IntWritable(max_temp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxTempRedo");

        // Jar name when running
        job.setJarByClass(MaxTempRedo.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(MaxTempRedoMap.class);
        job.setReducerClass(MaxTempRedoReduce.class);
        // Output of Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Output of Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// ------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d MaxTempRedo/ MaxTempRedo.java
// jar -cvf MaxTempRedo.jar -C MaxTempRedo/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/MaxTempRedo.jar org.apache.hadoop.ramapo.MaxTempRedo ~/input/MaxTempRedo/MaxTempRedo.txt ~/hadoop_output/MaxTempRedo