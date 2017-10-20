package experiment;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class ProcessorUtil {
    static NumberFormat formatter = new DecimalFormat("#0");

    public static String format(double d) {
        return formatter.format(d);
    }

    public static Double parseValue(String line, String prefix, String breakStr) {
        int pos = line.lastIndexOf(prefix);
        if (pos < 0) {
            return null;
        }

        int startPos = pos + prefix.length();
        int endPos = line.indexOf(breakStr, startPos);
        String sub = line.substring(startPos, endPos);
        return Double.valueOf(sub);
    }

    public static Double parseRuntimeValue(String line, String eval) {
        return parseValue(line, eval, "/");
    }

    public static Double sumUp(Double... values) {
        Double result = 0.0;
        for (Double value : values) {
            if (value != null) {
                result += value;
            }
        }
        return result;
    }
}
