package experiment;

import java.io.File;
import java.io.IOException;

class RepairObject extends StatObject {
    double repairTime;
    double batchRepairTime;
    double rewriteRepairTime;
    double rewriteTime;
    double sortPositionScanRepairTime;
    double sortPositionLookupRepairTime;
    double sortPositionScanupRepair;
    double sortRepairTime;

    void updateRewriteTime(Double value) {
        if (value != null && value > 0.1) {
            rewriteTime = Double.max(rewriteTime, value);
        }
    }

    void updateRepairTime(Double value) {
        if (value != null && value > 0.1) {
            repairTime = Double.max(repairTime, value);
        }
    }

    void updateBatchRepairTime(Double value) {
        if (value != null && value > 0.1) {
            batchRepairTime = Double.max(batchRepairTime, value);
        }
    }

    void updateRewriteRepairTime(Double value) {
        if (value != null && value > 0.1) {
            rewriteRepairTime = Double.max(rewriteRepairTime, value);
        }
    }

    void updateSortRepairTime(Double value) {
        if (value != null && value > 0.1) {
            sortRepairTime = Double.max(sortRepairTime, value);
        }
    }

    void updateSortPositionScanRepairTime(Double value) {
        if (value != null && value > 0.1) {
            sortPositionScanRepairTime = Double.max(sortPositionScanRepairTime, value);
        }
    }

    void updateSortPositionLookupRepairTime(Double value) {
        if (value != null && value > 0.1) {
            sortPositionLookupRepairTime = Double.max(sortPositionLookupRepairTime, value);
        }
    }

    void updateSortPositionScanupRepairTime(Double value) {
        if (value != null && value > 0.1) {
            sortPositionScanupRepair = Double.max(sortPositionScanupRepair, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ProcessorUtil.format(sortPositionScanupRepair));
        sb.append("\t");
        sb.append(ProcessorUtil.format(sortPositionLookupRepairTime));
        sb.append("\t");
        sb.append(ProcessorUtil.format(sortPositionScanRepairTime));
        sb.append("\t");
        sb.append(ProcessorUtil.format(sortRepairTime));
        sb.append("\t");
        sb.append(ProcessorUtil.format(rewriteRepairTime));
        sb.append("\t");
        sb.append(ProcessorUtil.format(rewriteTime));
        sb.append("\t");

        return sb.toString();
    }

    @Override
    public String toHeaderString() {
        return "ScanupRepair\tLookupRepair\tScanRepair\tSortRepair\tRewriteSortLookupRepair\tRewrite";
    }
}

/**
 * Parses experiment results of index scans
 *
 * @author chen.Luo1
 *
 */
public class RepairProcessor extends ResultProcessor<RepairObject> {

    public RepairProcessor(String inputPath) {
        super(inputPath);
    }

    public static void main(String[] args) throws IOException {
        new RepairProcessor("/Users/luochen/Documents/Research/experiments/results/ingest/rewrite-repair").run();
    }

    @Override
    protected RepairObject newStatObject() {
        return new RepairObject();
    }

    @Override
    protected void extractStatObject(String line, RepairObject object) {
        super.extractStatObject(line, object);

        object.updateRepairTime(ProcessorUtil.parseValue(line, "Standalone repair takes ", " "));
        object.updateBatchRepairTime(ProcessorUtil.parseValue(line, "Batch repair takes ", " "));
        object.updateRewriteRepairTime(
                ProcessorUtil.parseValue(line, "Rewrite sort position lookup repair takes ", " "));
        object.updateRewriteTime(ProcessorUtil.parseValue(line, "Rewrite takes ", " "));
        object.updateSortRepairTime(ProcessorUtil.parseValue(line, "Sort repair takes ", " "));
        object.updateSortPositionScanRepairTime(ProcessorUtil.parseValue(line, "Scan repair takes ", " "));
        object.updateSortPositionLookupRepairTime(ProcessorUtil.parseValue(line, "Lookup repair takes ", " "));
        object.updateSortPositionScanupRepairTime(ProcessorUtil.parseValue(line, "Scanup repair takes ", " "));

    }

    @Override
    protected boolean acceptFile(File file) {
        String fileName = file.getName();
        return fileName.contains("_") && !fileName.contains(".txt") && file.isFile();
    }

    @Override
    protected String[] parseFileName(File file) {
        String fileName = file.getName();
        String[] parts = fileName.split("_");
        String experiment = parts[0];
        String value = parts[parts.length - 1];
        return new String[] { experiment, value };
    }

}
