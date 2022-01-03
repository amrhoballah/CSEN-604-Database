import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {
    private String name;
    private Vector<Row> colNameValue;
    private Vector<Integer> references;

    public Bucket() {
        setColNameValue(new Vector<>());
        setReferences(new Vector<>());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Vector<Row> getColNameValue() {
        return colNameValue;
    }

    public void setColNameValue(Vector<Row> colNameValue) {
        this.colNameValue = colNameValue;
    }

    public Vector<Integer> getReferences() {
        return references;
    }

    public void setReferences(Vector<Integer> references) {
        this.references = references;
    }
}
