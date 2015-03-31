import java.io.IOException;
import java.util.*;
       
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class findMin {
        
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);
         
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String name = new String();
            String gender = new String();
            String age = new String();
            String score = new String();
            StringTokenizer tokenizer = new StringTokenizer(line, "::");
            if (tokenizer.hasMoreTokens())
                gender = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                name = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                score = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                age = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                tokenizer.nextToken();
	        word.set(gender);
            context.write(word, new Text(name + ":" + score + ":" + age));
            
        }
    } 

    public static class Xpartitioner extends Partitioner<Text, Text>{
        @Override
        public int getPartition(Text Key, Text value, int b){
            String [] nameScoreAge = value.toString().split(":");
            String comp3 = nameScoreAge[2];
            int comp3Int = Integer.parseInt(comp3);
            if (b == 0)
                return 0;
            if (comp3Int <= 20)
                return 0;
            if (comp3Int > 20 && comp3Int <= 40)
                return 1 % b;
            if (comp3Int > 40 && comp3Int <= 60)
                return 2 % b;
            else
                return 3 % b;
        }
    }
         
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
	private final static IntWritable seven = new IntWritable(7);

        public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            String name = "";
            String age = "";
            String gender = "";
            int score = 0;
            for (Text val: values){
                String [] valTokens = val.toString().split(":");
                score = Integer.parseInt(valTokens[1]);
                if (score > maxScore){
                    name = valTokens[0];
                    age = valTokens[2];
                    gender = key.toString();
                    maxScore = score;
                }
            }
            context.write(new Text(name), new Text("age-"+age+":"+gender+":score-"+maxScore));
        }

    }
         
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        Job job = new Job(conf, "findMin");
    	job.setJarByClass(findMin.class);
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         
        job.setMapperClass(Map.class);
        job.setPartitionerClass(Xpartitioner.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(2);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        job.waitForCompletion(true);
    }
         
}
