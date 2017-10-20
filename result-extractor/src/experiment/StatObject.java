package experiment;

public class StatObject {
    double total = 0;

    void updateTotal(Double value) {
        if (value != null && value >= 0.1) {
            total = Double.max(total, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ProcessorUtil.format(total));
        sb.append("\t");
        return sb.toString();
    }

    public String toHeaderString() {
        return "Total";

    }
}