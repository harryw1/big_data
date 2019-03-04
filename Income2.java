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

public class Income2{
    public static class IncomeMap extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            int age = Integer.parseInt(linevalues[0]);
            String age_group = "";
            if (age >= 10 && age <= 19) {age_group = "10-19";}
            else if (age >= 20 && age <= 29) {age_group = "20-29";}
            else if (age >= 30 && age <= 39) {age_group = "30-39";}
            else if (age >= 40 && age <= 49) {age_group = "40-49";}
            else if (age >= 50 && age <= 59) {age_group = "50-59";}
            else if (age >= 60 && age <= 69) {age_group = "60-69";}
            else if (age >= 70 && age <= 79) {age_group = "70-79";}
            else if (age >= 80 && age <= 89) {age_group = "80-89";}
            else if (age >= 90 && age <= 99) {age_group = "90-99";}
            context.write(new Text(age_group), new Text(linevalues[14]));
        }
    }

    public static class IncomeReduce extends Reducer<Text, Text, Text, Text>{
        String income = "";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int income_count = 0;
            for (Text val:values){
                income = val.toString();
                if (income != "<=50K"){
                    income_count += 1;
                }
            }
            context.write(key, new Text(Integer.toString(income_count)));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Income2");

        // Jar name when running
        job.setJarByClass(Income2.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(IncomeMap.class);
        job.setReducerClass(IncomeReduce.class);
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
// javac -classpath ${HADOOP_CLASSPATH} -d Income2/ Income2.java
// jar -cvf Income2.jar -C Income2/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/Income2.jar 
// org.apache.hadoop.ramapo.Income2 ~/input/Income/Income.txt ~/hadoop_output/Income2