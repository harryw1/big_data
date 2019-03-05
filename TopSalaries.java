// Using the TreeMap data structure, we can sort
// employees and salaries before mapping and reducing
// to cut down on expensive operations.

package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
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

public class TopSalaries {
    public static class TopSalariesMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        // TreeMap Object to contain and sort the data
        // before passing it to the mapper
        private TreeMap<Integer, Text> treemap = new TreeMap();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] linevalues = value.toString().split(",");
            Integer val = Integer.parseInt(linevalues[1]);

            treemap.put(val, new Text(linevalues[0]));
            // Remove values from the tree when we have
            // more than three values
            if (treemap.size() > 3) {
                treemap.remove(treemap.firstKey());
            }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Text> entry : treemap.entrySet()) {
                context.write(new IntWritable(entry.getKey()), entry.getValue());
            }
        }
    }

    public static class TopSalariesReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, new Text(val));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopSalaries");

        // Jar name when running
        job.setJarByClass(TopSalaries.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(TopSalariesMap.class);
        job.setReducerClass(TopSalariesReduce.class);
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

// ------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d TopSalaries/ TopSalaries.java
// jar -cvf TopSalaries.jar -C TopSalaries/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar
// /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/TopSalaries.jar
// org.apache.hadoop.ramapo.TopSalaries ~/input/TopSalaries/TopSalaries.txt
// ~/hadoop_output/TopSalaries