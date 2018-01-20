package edu.uci.asterixdb.storage.experiments.index.query;

public abstract class AggregateQueryAction extends QueryAction {

    protected String batchMemory;

    public AggregateQueryAction(String dataverse, String dataset, Runnable preRun) {
        this(dataverse, dataset, preRun, null);
    }

    public AggregateQueryAction(String dataverse, String dataset, Runnable preRun, String batchMemory) {
        super(dataverse, dataset, preRun);
        this.batchMemory = batchMemory;
    }

    protected String getSetMemoryStatement() {
        if (batchMemory == null || batchMemory.isEmpty()) {
            return "";
        }
        return "SET `compiler.batchmemory` " + '"' + batchMemory + '"' + ";\n";
    }

}
