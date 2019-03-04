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

public class HeartDisease{
    public static class HeartDiseaseMap extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String [] linevalues = line.toString().split(",");
            int age = Integer.parseInt(linevalues[0]);
            String age_group = "";
            if (age >= 20 && age <= 29) {age_group = "20-29";}
            else if (age >= 30 && age <= 39) {age_group = "30-39";}
            else if (age >= 40 && age <= 49) {age_group = "40-49";}
            else if (age >= 50 && age <= 59) {age_group = "50-59";}
            else if (age >= 60 && age <= 69) {age_group = "60-69";}
            context.write(new Text(age_group), new Text(linevalues[1]+","+linevalues[13]));
        }
    }

    public static class HeartDiseaseReduce extends Reducer<Text, Text, Text, Text>{
        private int f_num_heart_disease = 0;
        private int m_num_heart_disease = 0;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String [] linevalues;
            
            for(Text val:values){
                linevalues = val.toString().split(",");
                if(Integer.parseInt(linevalues[1]) == 1){
                    if(Integer.parseInt(linevalues[0]) == 0){
                        f_num_heart_disease += 1;
                    }
                    if(Integer.parseInt(linevalues[0]) == 1){
                        m_num_heart_disease += 1;
                    }
                }
            }
            context.write(key, new Text(f_num_heart_disease+", "+m_num_heart_disease));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie List");

        // Jar name when running
        job.setJarByClass(HeartDisease.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(HeartDiseaseMap.class);
        job.setReducerClass(HeartDiseaseReduce.class);
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
// javac -classpath ${HADOOP_CLASSPATH} -d <FILENAME>/ <FILENAME>.java
// jar -cvf <FILENAME>.jar -C <FILENAME>/ .

// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/<FILENAME>.jar org.apache.hadoop.ramapo.<FILENAME> ~/input/<FILENAME>.txt ~/<OUTPUTFOLDER>