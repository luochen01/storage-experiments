package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class ParamComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        try {
            int i1 = Integer.valueOf(o1);
            int i2 = Integer.valueOf(o2);
            return Integer.compare(i1, i2);
        } catch (Exception e) {
            return o1.compareTo(o2);
        }
    }
}

/**
 * By default, A ResultProcessor parses each result file as a stat object,
 * and print them as a table format
 *
 * @author chen.Luo1
 *
 * @param <T>
 */
public abstract class ResultProcessor<T extends StatObject> {
    protected Map<String, Map<String, StatObject>> resultMap = new HashMap<>();

    protected String inputPath;

    public ResultProcessor(String inputPath) {
        this.inputPath = inputPath;
    }

    public void run() throws IOException {
        extractResults(inputPath);
    }

    protected void extractResults(String dirPath) throws IOException {
        File dir = new File(dirPath);
        for (File file : dir.listFiles()) {
            String fileName = file.getName();
            if (fileName.contains("-") && !fileName.contains(".txt") && file.isFile()) {

                String experiment = fileName.substring(0, fileName.indexOf("-"));
                String value = fileName.substring(fileName.indexOf("-") + 1);
                StatObject object = extractStatObject(file);

                addResult(experiment, value, object);
            }
        }
        writeResult(dirPath);
    }

    protected void addResult(String experiment, String method, StatObject object) {
        Map<String, StatObject> map = resultMap.get(experiment);
        if (map == null) {
            map = new TreeMap<String, StatObject>(new ParamComparator());
            resultMap.put(experiment, map);
        }
        map.put(method, object);
    }

    protected StatObject extractStatObject(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        System.out.println("processing file " + file.getAbsolutePath());
        String line;
        T object = newStatObject();
        while ((line = reader.readLine()) != null) {
            extractStatObject(line, object);
        }
        reader.close();
        return object;
    }

    protected void writeResult(String dir) throws IOException {
        for (String experiment : resultMap.keySet()) {
            File file = new File(dir, experiment + ".txt");
            PrintWriter writer = new PrintWriter(file);
            Map<String, StatObject> map = resultMap.get(experiment);
            List<String> values = new ArrayList<String>(map.keySet());

            writer.print("parameter");
            writer.print("\t");
            writer.println(newStatObject().toHeaderString());
            for (String value : values) {
                StatObject obj = map.get(value);
                writer.print(value);
                writer.print("\t");
                writer.println(obj);
            }
            writer.close();
            System.out.println("Generate " + file.getPath());
        }
    }

    protected abstract T newStatObject();

    protected void extractStatObject(String line, T object) {
        Double total = ProcessorUtil.parseValue(line, "Total finishes in ", " ");
        object.updateTotal(total);

    }

}
