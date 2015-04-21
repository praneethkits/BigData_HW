
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
            int n = genres.length;
            int i = n;
            while (i >= 1){
                int k = n - i + 1;
                if (i ==  2)
                    retString = retString + k + ") " + genres[i-1] + " & ";
                else if (i == 1)
                    retString = retString + k + ") " + genres[i-1] + " ";
                else
                    retString = retString + k + ") " + genres[i-1] + ", ";
                i -= 1;
            }
            retString = retString + "sxm132831 :hive";
        } catch (Exception e) { 
            retString = "FUNCTION FAILED";
        }
        return new Text(retString);
    }
}

