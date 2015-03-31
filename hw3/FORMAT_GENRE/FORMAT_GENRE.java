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
            String retString = "";
            int n = genres.length;
            int i = n;
            while (i >= 1){
                int k = n - i + 1;
                if (i ==  2)
                    retString = retString + k + ") " + genres[i-1].trim() + " & ";
                else if (i == 1)
                    retString = retString + k + ") " + genres[i-1].trim() + " ";
                else
                    retString = retString + k + ") " + genres[i-1].trim() + ", ";
                i -= 1;
            }

            retString = retString + "pxv130830";
            return retString;
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
