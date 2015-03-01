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
        
public class MaleUsers7 {
        
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
         
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String uid = new String();
            String gender = new String();
            IntWritable age = new IntWritable(0);
            StringTokenizer tokenizer = new StringTokenizer(line, "::");
            if (tokenizer.hasMoreTokens())
                uid = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                gender = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                age = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
            if (tokenizer.hasMoreTokens())
                tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                tokenizer.nextToken();
	    if (gender.equals("M")){
                word.set(uid);
                context.write(word, age);
            }
            
        }
    } 
         
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
	private final static IntWritable seven = new IntWritable(7);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
            for (IntWritable val : values) {
                int age = val.get();
                if (age == 7){
                    context.write(key, new Text(" "));
                }
            }
        }
    }
         
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        Job job = new Job(conf, "maleusers7");
    	job.setJarByClass(MaleUsers7.class);
     
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
