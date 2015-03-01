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
        
public class AgeGroup {
        
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);
         
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String uid = new String();
            String gender = new String();
            String age = new String();
            StringTokenizer tokenizer = new StringTokenizer(line, "::");
            if (tokenizer.hasMoreTokens())
                uid = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                gender = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                age = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                tokenizer.nextToken();
	    word.set(age + " " + gender);
            context.write(word, one);
            
        }
    } 
         
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final static IntWritable seven = new IntWritable(7);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
               sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
         
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        Job job = new Job(conf, "agegroup");
    	job.setJarByClass(AgeGroup.class);
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        job.waitForCompletion(true);
    }
         
}
