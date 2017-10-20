package experiment;

import java.io.IOException;

class SortObject extends StatObject {
    double sortTime = 0;
    double mergeTime = 0;

    void updateSortTime(Double value) {
        if (value != null && value > 0.1) {
            sortTime = Double.max(sortTime, value);
        }
    }

    void updateMergeTime(Double value) {
        if (value != null && value > 0.1) {
            mergeTime = Double.max(mergeTime, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ProcessorUtil.format(total));
        sb.append("\t");
        sb.append(ProcessorUtil.format(sortTime));
        sb.append("\t");
        sb.append(ProcessorUtil.format(mergeTime));
        sb.append("\t");
        return sb.toString();
    }

    @Override
    public String toHeaderString() {
        return "Total\tSort\tMerge";
    }
}

/**
 * Parses experiment results of index scans
 *
 * @author chen.Luo1
 *
 */
public class SortProcessor extends ResultProcessor<SortObject> {

    public SortProcessor(String inputPath) {
        super(inputPath);
    }

    public static void main(String[] args) throws IOException {
        new SortProcessor("/Users/luochen/Documents/Research/experiments/results/sort-sensorium-before").run();
    }

    @Override
    protected SortObject newStatObject() {
        return new SortObject();
    }

    @Override
    protected void extractStatObject(String line, SortObject object) {
        super.extractStatObject(line, object);

        object.updateTotal(ProcessorUtil.parseValue(line, "Total finishes in ", " "));

        object.updateSortTime(ProcessorUtil.parseValue(line, "Sort finishes in ", " "));
        object.updateMergeTime(ProcessorUtil.parseValue(line, "Merge finishes in ", " "));

    }

}
