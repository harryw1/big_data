// Movie Recommendation
package org.apache.hadoop.ramapo;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class MovieRecommender{
    // Hash map with user ids and a list of all of the movies they like as the keys
    private static HashMap<Integer, ArrayList<Integer>> user_id_likes_list = new HashMap<Integer, ArrayList<Integer>>();
    // Hash map with user ids and a list of all of their friends
    private static HashMap<Integer, ArrayList<Integer>> friends_list = new HashMap<Integer, ArrayList<Integer>>();
    // List with all of the user ids in it
    private static HashMap<Integer, Integer> user_id_list = new HashMap<Integer, Integer>(); 

    public static class MovieRecommenderMAP extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] linevalues = value.toString().split(",");
            context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(linevalues[1] + "," + linevalues[2]));
        }
    }

    public static class MovieRecommenderREDUCE extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer user_id = Integer.parseInt(key.toString());
            user_id_list.put(user_id, 1);
            create_likes_list(key, values);
        }

        public void create_likes_list(IntWritable key, Iterable<Text> values) throws IOException, InterruptedException{
            Float movie_rating;
            Integer user_id = Integer.parseInt(key.toString());
            for (Text val : values){
                String [] linevalues = val.toString().split(",");
                movie_rating = Float.parseFloat(linevalues[1]);
                ArrayList<Integer> temp_likes;
                if (movie_rating >= 3.0){
                    if (user_id_likes_list.containsKey(user_id)){
                        temp_likes = user_id_likes_list.get(key.get());
                        temp_likes.add(Integer.valueOf(linevalues[0]));
                    }
                    else{
                        temp_likes = new ArrayList<Integer>();
                        temp_likes.add(Integer.valueOf(linevalues[0]));
                        user_id_likes_list.put(key.get(), temp_likes);
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            create_friends_list();
            for(HashMap.Entry<Integer, Integer> user : user_id_list.entrySet()){
                Integer user_id = user.getKey();
                recommendation(user_id, context);
            }
        }

        public void create_friends_list(){
            for(HashMap.Entry<Integer, ArrayList<Integer>> user : user_id_likes_list.entrySet()){
                Integer user_id = user.getKey();
                ArrayList<Integer> likes = user.getValue();

                for(Integer like : likes){
                    for(HashMap.Entry<Integer, ArrayList<Integer>> friend : user_id_likes_list.entrySet()){
                        Integer f_id = friend.getKey();
                        ArrayList<Integer> friend_likes = friend.getValue();

                        if(user_id != f_id){
                            if(friend_likes.contains(like)){
                                if(friends_list.get(user_id) == null){
                                    friends_list.put(user_id, new ArrayList<Integer>());
                                }
                                if(!friends_list.get(user_id).contains(f_id)){
                                    friends_list.get(user_id).add(f_id);
                                }
                            }
                        }
                    }
                }
            }
        }

        public void recommendation(Integer id, Context context) throws IOException, InterruptedException{
            ArrayList<Integer> user_likes = new ArrayList<Integer>();
            for(HashMap.Entry<Integer, ArrayList<Integer>> user : user_id_likes_list.entrySet()){
                Integer user_id = user.getKey();
                if(user_id == id){
                    user_likes = user.getValue();
                }
            }

            for(HashMap.Entry<Integer, ArrayList<Integer>> user : friends_list.entrySet()){
                Integer user_id = user.getKey();
                if(user_id == id){
                    ArrayList<Integer> friends = user.getValue();

                    for(Integer friend_id : friends){
                        for(HashMap.Entry<Integer, ArrayList<Integer>> friend : user_id_likes_list.entrySet()){
                            Integer f_id = friend.getKey();
                            if(friend_id == f_id){
                                ArrayList<Integer> friend_likes = friend.getValue();
                                for(Integer liked_movie : friend_likes){
                                    if(user_likes.contains(liked_movie)){}
                                    else{context.write(new IntWritable(user_id), new IntWritable(liked_movie));}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MovieRecommender");

        // Jar name when running
        job.setJarByClass(MovieRecommender.class);
        // Mapper and Reducer Class Setting
        job.setMapperClass(MovieRecommenderMAP.class);
        job.setReducerClass(MovieRecommenderREDUCE.class);
        // Output of Mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        // Output of Reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // File Input/Output path setting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//------------------------------------------------------------------
// export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
// javac -classpath ${HADOOP_CLASSPATH} -d MovieRecommender/ MovieRecommender.java
// jar -cvf MovieRecommender.jar -C MovieRecommender/ .
// ****************************
// THIS IS ONE LINE IN TERMINAL
// ****************************
// /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/mywordcount/MovieRecommender.jar 
// org.apache.hadoop.ramapo.MovieRecommender ~/input/MovieRecommender/MovieRecommender.txt ~/hadoop_output/MovieRecommender