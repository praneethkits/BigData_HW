import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class FORMAT_GENRE extends EvalFunc<String>
{
     public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String genre = (String)input.get(0);
            String genres[] = genre.split("\\|");
            int i = 1;
            String retString = "";
            int n = genres.length;
            while (i <= n){
                if (i == n-1)
                    retString = retString + " " + i + ") " + genres[i-1].trim() + " &";
                else if (i == n)
                    retString = retString + " " + i + ") " + genres[i-1].trim() + " ";
                else
                    retString = retString + " " + i + ") " + genres[i-1].trim() + ",";
                i += 1;
            }
            retString = retString + "pxv130830";
            return retString;
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
