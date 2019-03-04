package org.apache.hadoop.ramapo;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendsList
{
    public static class MyMapper extends Mapper <LongWritable, Text, Text, Text>
    {
        // private final static IntWritable one = new IntWritable(1);
        // private Text uID = new Text();
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String [] linevalues = value.toString().split(",");
            // StringTokenizer it = new StringTokenizer(value.toString());
            // uID.set(it.nextToken());
            context.write(new Text(linevalues[0]), new Text(linevalues[1]+ "," + "1"));
        }
    }

    public static class IntSumReducer extends Reducer <Text, Text, Text, IntWritable>
    {
        // private IntWritable result = new IntWritable();
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String [] linevalues;
            Integer sum = 0;

            for (Text val:values){
                linevalues = val.toString().split(",");
                sum += Integer.parseInt(linevalues[1]);
            }
            // int numFriends = 0;
            // for(IntWritable value : values)
            // {
            //     numFriends += value.get();
            // }
            // result.set(numFriends);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friends List");
        job.setJarByClass(FriendsList.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]) );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d <FILENAME>/ <FILENAME>.java
// jar -cvf <FILENAME>.jar -C <FILENAME>/ .

// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/<FILENAME>.jar org.apache.hadoop.ramapo.<FILENAME> ~/input/<FILENAME>.txt ~/<OUTPUTFOLDER>
