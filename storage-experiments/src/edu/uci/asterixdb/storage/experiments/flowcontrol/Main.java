package edu.uci.asterixdb.storage.experiments.flowcontrol;

public class Main {

    public static void main(String[] args) {
        new MinLimitFlowControlSimulator(3600 * 3).execute(true);
    }

}
