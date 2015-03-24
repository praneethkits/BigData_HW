
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(name = "format_genre",
value = "_FUNC_(str) - splits genres into different genres",
extended = "Example:\n"
+ "  > SELECT format_genre(genres) FROM movies;")

public class hiveFormatGenre extends UDF {
    public Text evaluate(Text input) {
        if (input == null)
            return new Text("");

        String retString = "";
        try {
            String genre = input.toString();
            String genres[] = genre.split("\\|");
            int i = 1;
            int n = genres.length;
            while (i <= n){
                if (i ==  n-1)
                    retString = retString + i + ") " + genres[i-1] + " & ";
                else if (i == n)
                    retString = retString + i + ") " + genres[i-1] + " ";
                else
                    retString = retString + i + ") " + genres[i-1] + ", ";
                i += 1;
            }
            retString = retString + "pxv130830";
        } catch (Exception e) { 
            retString = "FUNCTION FAILED";
        }
        return new Text(retString);
    }
}

