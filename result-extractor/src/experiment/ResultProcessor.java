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
        extractResults();
    }

    protected void extractResults() throws IOException {
        File dir = new File(inputPath);
        for (File file : dir.listFiles()) {
            if (acceptFile(file)) {
                String[] strs = parseFileName(file);
                StatObject object = extractStatObject(file);
                addResult(strs[0], strs[1], object);
            }
        }
        writeResult();
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

    protected void writeResult() throws IOException {
        for (String experiment : resultMap.keySet()) {
            File file = getOutputFile(experiment);
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
            writeExtraFile(experiment, map);
            System.out.println("Generate " + file.getPath());
        }
    }

    protected File getOutputFile(String name) {
        return new File(inputPath, name + ".txt");
    }

    protected abstract T newStatObject();

    protected void writeExtraFile(String experiment, Map<String, StatObject> map) throws IOException {

    }

    protected void extractStatObject(String line, T object) {
        Double total = ProcessorUtil.parseValue(line, "Total finishes in ", " ");
        object.updateTotal(total);

    }

    protected boolean acceptFile(File file) {
        String fileName = file.getName();
        return fileName.contains("-") && !fileName.contains(".txt") && file.isFile();
    }

    protected String[] parseFileName(File file) {
        String fileName = file.getName();
        String experiment = fileName.substring(0, fileName.indexOf("-"));
        String value = fileName.substring(fileName.indexOf("-") + 1);
        return new String[] { experiment, value };
    }

}
