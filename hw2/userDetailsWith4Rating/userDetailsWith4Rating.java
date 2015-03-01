import java.net.URI;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class userDetailsWith4Rating extends Configured implements Tool {
 
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4 ){
            System.out.printf("Four Parameters required <users file> <ratings file> <movie Id> <output path>\n");
            return -1;
        }
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        conf.set("movieId", args[2]);
        job.setJobName("userDetailsWith4Rating");
        DistributedCache.addCacheFile(new URI(args[0]), conf);
        job.setJarByClass(userDetailsWith4Rating.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setMapperClass(userDetailsWith4RatingMap.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new userDetailsWith4Rating(), args));
    }

}
