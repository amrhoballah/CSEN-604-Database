import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    private String tableName;
    private Vector<Row> tuples;

    public Table(String tableName){
        this.setTableName(tableName);
        this.setTuples(new Vector());
    }

    public void print(){
        System.out.println("table name: " + getTableName() + "\ntuples: ");
        for (Row row : getTuples()) {
            System.out.println(row.getColNameValue());
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Vector<Row> getTuples() {
        return tuples;
    }

    public void setTuples(Vector<Row> tuples) {
        this.tuples = tuples;
    }
}
