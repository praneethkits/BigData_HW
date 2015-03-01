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
        
public class MovieGenre {
    

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        private static String genre;
         
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            genre = conf.get("genre");
            String line = value.toString();
            String mid = new String();
            String title = new String();
            String genres = new String();
            boolean found = false;
            StringTokenizer tokenizer = new StringTokenizer(line, "::");
            if (tokenizer.hasMoreTokens())
                mid = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                title = tokenizer.nextToken();
            if (tokenizer.hasMoreTokens())
                genres = tokenizer.nextToken();

            StringTokenizer subTokenizer = new StringTokenizer(genres, "|");
            while (subTokenizer.hasMoreTokens()){
                String subGenre = subTokenizer.nextToken();
                if (subGenre.trim().toUpperCase().equals(genre.trim().toUpperCase())){
                    word.set(title);
                    found = true;
                    break;
                }
            }
            if (found)
                context.write(word, one);
            
        }
    } 
         
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable val : values) {
               sum += val.get();
            }
            context.write(key, new Text(" "));
        }
    }
         
    public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
	conf.set("genre", args[2]);

        Job job = new Job(conf, "moviegenre");
    	job.setJarByClass(MovieGenre.class);
     
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
