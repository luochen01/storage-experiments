package edu.uci.asterixdb.storage.experiments.query;

import java.util.ArrayList;
import java.util.List;

public class QueryGroup {

    private final String name;

    private final List<IQueryAction> actions = new ArrayList<>();

    public QueryGroup(String name) {
        this.name = name;
    }

    public void addAction(IQueryAction action) {
        this.actions.add(action);
    }

    public List<QueryResult> run() {
        List<QueryResult> results = new ArrayList<>();
        for (IQueryAction action : actions) {
            try {
                QueryResult result = action.run();
                System.out.println(result);
                results.add(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    public String getName() {
        return name;
    }
}
