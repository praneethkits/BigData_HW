import java.io.IOException;
import java.util.*;
       
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class top5AvgbyFemale {
        
    public static class femaleUsersMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] userFields = line.split("::");
            if (userFields[1].equals("F") ){
                context.write(new Text(userFields[0]), new Text("UF::"));
            }
        }
    } 

    public static class movieNameMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] movieFields = line.split("::");
            context.write(new Text(movieFields[0]), new Text("MF::" + movieFields[1]));
        }
    }

    public static class tempOutMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] outFields = line.split("::");
            context.write(new Text(outFields[0]), new Text("TF::" + outFields[1]));
        }
    }


    public static class ratingsMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] ratingFields = line.split("::");
            context.write(new Text(ratingFields[0]), new Text("RF::" + ratingFields[1] + "::" + ratingFields[2]));
        }
    }

    public static class finalMap extends Mapper<LongWritable, Text, FloatWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String [] fields = line.split("::");
            float key1 = Float.parseFloat(fields[1]);
            FloatWritable keys = new FloatWritable(-1 * key1);
            context.write(keys, new Text(fields[0]));
        }
    }

         
    public static class userRatingsReduce extends Reducer<Text, Text, Text, Text> {

        public class writeObjects{
            String movieId;
            String rating;
            writeObjects(String m1, String r1){
                movieId = new String();
                movieId = m1;
                rating = new String();
                rating = r1;
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
            boolean found = false;
            //String uid = new String();
            List<writeObjects> writeList = new ArrayList<writeObjects>();
            String movieId = new String();
            String rating = new String();
            for (Text val : values) {
                String [] cols = val.toString().split("::");
                if (cols[0].equals("UF")){
                    found = true;
                }else if (cols[0].equals("RF")){
                    movieId = cols[1];
                    rating = cols[2];
                    writeObjects wO = new writeObjects(movieId, rating);
                    writeList.add(wO);
                } 
            }
            if (found){
                for(writeObjects w1: writeList){
                    context.write(new Text(w1.movieId + "::" + w1.rating), new Text(" "));
                }
            }
        }
    }

    public static class movieRatingsReducer extends Reducer<Text, Text, Text, FloatWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String movieName = new String();
            int sum = 0;
            int count = 0;
            for (Text val : values){
                String [] cols = val.toString().trim().split("::");
                if (cols[0].equals("MF")){
                    movieName = cols[1].trim();
                }else{
                    count++;
                    sum = sum + Integer.parseInt(cols[1].trim());   
                }
            }
            float avg = 0;

            if (count != 0){
                avg = (float)(sum * 1.0)/count;
                FloatWritable average = new FloatWritable(avg);
                context.write(new Text(movieName + "::"), average);
            }
        }

    }


    public static class finalReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    
        private static int count = 0;
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            float vals = (float)-1.0 * key.get();
            for (Text val: values){
                if (count < 5){
                    context.write(val, new FloatWritable(vals));
                    count++;
                }
            }
        }
    }

         
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Path tempOutput = new Path("/tmp/userRatingsReduce_out");
        Path tempOutput_1 = new Path("/tmp/userRatingsReduce_out1");
 
        Job job = new Job(conf, "top5AvgbyFemale");
    	job.setJarByClass(top5AvgbyFemale.class);
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         
        //job.setMapperClass(Map.class);
        job.setReducerClass(userRatingsReduce.class);
         
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
         

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, femaleUsersMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ratingsMap.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempOutput);
         
        job.waitForCompletion(true);

        // Job 2 for joining the movies with temp ouput and calculating the avaerage ratings.

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf, "top5AvgbyFemale1");
        job2.setJarByClass(top5AvgbyFemale.class);
        
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, movieNameMap.class);
        MultipleInputs.addInputPath(job2, tempOutput, TextInputFormat.class, tempOutMap.class);

        job2.setOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setReducerClass(movieRatingsReducer.class);
        FileOutputFormat.setOutputPath(job2, tempOutput_1);

        job2.waitForCompletion(true);
        FileSystem fs1 = FileSystem.get(conf2);
        fs1.delete(tempOutput, true);

        // Job 3 for retrieving the top 5 user ratings.

        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf, "top5AvgbyFemale2");
        job3.setJarByClass(top5AvgbyFemale.class);

        job3.setMapOutputKeyClass(FloatWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);

        job3.setMapperClass(finalMap.class);
        job3.setReducerClass(finalReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, tempOutput_1);
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.waitForCompletion(true);
        fs1.delete(tempOutput_1, true);
    }
         
}
