package experiment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class IngestObject {
    long ingestedRecords;
    long totalRecords;

    public IngestObject(long ingestedRecords, long totalRecords) {
        super();
        this.ingestedRecords = ingestedRecords;
        this.totalRecords = totalRecords;
    }

}

class InplaceObject extends StatObject {

    List<IngestObject> ingestObjects = new ArrayList<>();

    double mergeTime;
    double catchUpTime;
    double totalRecords;
    double totalCatchUpRecords;

    void updateTotalCatchUpRecords(Double value) {
        if (value != null && value > 0.1) {
            totalCatchUpRecords = Double.max(totalCatchUpRecords, value);
        }
    }

    void updateTotalRecords(Double value) {
        if (value != null && value > 0.1) {
            totalRecords = Double.max(totalRecords, value);
        }
    }

    void updateMergeTime(Double value) {
        if (value != null && value > 0.1) {
            mergeTime = Double.max(mergeTime, value);
        }
    }

    void updateCatchupTime(Double value) {
        if (value != null && value > 0.1) {
            catchUpTime = Double.max(catchUpTime, value);
        }
    }

    void addIngestObject(IngestObject obj) {
        ingestObjects.add(obj);
    }

    @Override
    public String toString() {
        return ProcessorUtil.format(mergeTime) + "\t" + ProcessorUtil.format(catchUpTime) + "\t" + totalRecords + "\t"
                + totalCatchUpRecords;
    }

    @Override
    public String toHeaderString() {
        return "MergeTime\tCatchupTime\tTotalRecords\tCatchUpRecords";
    }
}

/**
 * Parses experiment results of index scans
 *
 * @author chen.Luo1
 *
 */
public class InplaceProcessor extends ResultProcessor<InplaceObject> {

    public InplaceProcessor(String inputPath) {
        super(inputPath);
    }

    public static void main(String[] args) throws IOException {
        new InplaceProcessor("/Users/luochen/Documents/Research/experiments/results/inplace/concurrency").run();
    }

    @Override
    protected InplaceObject newStatObject() {
        return new InplaceObject();
    }

    @Override
    protected void extractStatObject(String line, InplaceObject object) {
        super.extractStatObject(line, object);

        object.updateTotalCatchUpRecords(ProcessorUtil.parseValue(line, "total catch up tuples: ", ","));
        object.updateTotalRecords(ProcessorUtil.parseValue(line, "Upserted ", " "));
        object.updateMergeTime(ProcessorUtil.parseValue(line, "Merge takes ", " "));
        object.updateCatchupTime(ProcessorUtil.parseValue(line, "Finished catch up phase in ", " "));
        if (line.startsWith("Ingestion speed: ")) {
            String str = line.substring(line.indexOf(":") + 1);
            String[] parts = str.split("\t");
            object.addIngestObject(new IngestObject(Long.valueOf(parts[1]), Long.valueOf(parts[2])));
        }
    }

    @Override
    protected boolean acceptFile(File file) {
        String fileName = file.getName();
        return fileName.contains("_") && !fileName.contains(".txt") && !fileName.startsWith(".") && file.isFile();
    }

    @Override
    protected String[] parseFileName(File file) {
        String fileName = file.getName();
        String[] parts = fileName.split("_");
        String value = parts[1];
        StringBuilder experiment = new StringBuilder();
        experiment.append(parts[0]);
        experiment.append("_");
        for (int i = 2; i < parts.length; i++) {
            experiment.append(parts[i]);
            if (i < parts.length - 1) {
                experiment.append("_");
            }
        }
        return new String[] { experiment.toString(), value };
    }
    //
    //    @Override
    //    protected void writeExtraFile(String experiment, Map<String, StatObject> map) throws IOException {
    //        File file = getOutputFile(experiment + "-ingest-records");
    //        FileWriter writer = new FileWriter(file);
    //        boolean hasRemaining = true;
    //        int i = 0;
    //        List<String> keys = new ArrayList<>(map.keySet());
    //        Collections.sort(keys);
    //
    //        StringBuilder header = new StringBuilder();
    //        header.append("time\t");
    //        for (String key : keys) {
    //            header.append(key);
    //            header.append("\t");
    //        }
    //        writer.write(header.toString());
    //        writer.write(System.lineSeparator());
    //        while (hasRemaining) {
    //            hasRemaining = false;
    //            StringBuilder line = new StringBuilder();
    //            line.append(i);
    //            line.append("\t");
    //            for (String key : keys) {
    //                InplaceObject stat = (InplaceObject) map.get(key);
    //                if (i < stat.ingestObjects.size()) {
    //                    line.append(stat.ingestObjects.get(i).ingestedRecords);
    //                    line.append('\t');
    //                    hasRemaining = true;
    //                } else {
    //                    line.append('-');
    //                    line.append('\t');
    //                }
    //            }
    //            writer.write(line.toString());
    //            writer.write(System.lineSeparator());
    //            i++;
    //        }
    //
    //        writer.close();
    //
    //        file = getOutputFile(experiment + "-ingest-total");
    //        writer = new FileWriter(file);
    //        writer.write(header.toString());
    //        writer.write(System.lineSeparator());
    //        hasRemaining = true;
    //        i = 0;
    //        while (hasRemaining) {
    //            hasRemaining = false;
    //            StringBuilder line = new StringBuilder();
    //            line.append(i);
    //            line.append("\t");
    //            for (String key : keys) {
    //                InplaceObject stat = (InplaceObject) map.get(key);
    //                if (i < stat.ingestObjects.size()) {
    //                    line.append(stat.ingestObjects.get(i).totalRecords);
    //                    line.append('\t');
    //                    hasRemaining = true;
    //                } else {
    //                    line.append('-');
    //                    line.append('\t');
    //                }
    //            }
    //            i++;
    //            writer.write(line.toString());
    //            writer.write(System.lineSeparator());
    //        }
    //        writer.close();
    //    }
}
