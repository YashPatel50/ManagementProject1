import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class AGEGROUP extends EvalFunc<String>{
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        int age = (Integer) input.get(0);

        if (age < 20) {
            return "[10-20)";
        } else if (age < 30) {
            return "[20-30)";
        } else if (age < 40) {
            return "[30-40)";
        } else if (age < 50) {
            return "[40-50)";
        } else if (age < 60) {
            return "[50-60)";
        } else if (age <= 70) {
            return "[60-70]";
        } else {
            return "COULD NOT GROUP";
        }
    }
}
