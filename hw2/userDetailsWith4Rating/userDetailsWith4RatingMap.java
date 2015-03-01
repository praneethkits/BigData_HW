import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;

public class userDetailsWith4RatingMap extends
    Mapper<LongWritable, Text, Text, Text> {

    private static HashMap<String, String> userHashMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
 
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
 
        for (Path eachPath : cacheFilesLocal) {
            loadUserHashMap(eachPath, context);
            }
        }
 


    private void loadUserHashMap(Path filePath, Context context)
            throws IOException {

        BufferedReader bR;
        String line = "";
        bR = new BufferedReader(new FileReader(filePath.toString()));
        while ((line = bR.readLine()) != null) {
            String [] userFields = line.split("::");
            userHashMap.put(userFields[0].trim(), userFields[1].trim() + "::" + userFields[2].trim());
        }
        bR.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String movieId = conf.get("movieId");
        movieId = movieId.trim();
        String line = value.toString();
        String [] ratingFields = line.split("::");
        int rating = Integer.parseInt(ratingFields[2].trim());
        if (movieId.equals(ratingFields[1].trim()) && rating >= 4){
            String userId = ratingFields[0].trim();
            String [] userFields = userHashMap.get(userId).split("::");
            context.write(new Text(userId), new Text(userFields[0].trim() + "\t" + userFields[1].trim() ));
        }
    }
}

