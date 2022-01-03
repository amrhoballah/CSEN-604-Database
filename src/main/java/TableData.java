import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class TableData {
    private String tableName;
    private String key;
    private Hashtable<String, String> colNameType;
    private Hashtable<String, String> colNameMin;
    private Hashtable<String, String> colNameMax;
    private int indexCount = 0;
    private Vector<Vector<String>> indexName = new Vector();
    private int pages = 0;
    private Vector max = new Vector();
    private Vector<Integer> size = new Vector<>();
    private Vector<Integer> hasOverflow = new Vector<>();

    public TableData() {
    };

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }

    public void setColNameType(Hashtable<String, String> colNameType) {
        this.colNameType = colNameType;
    }

    public Hashtable<String, String> getColNameMin() {
        return colNameMin;
    }

    public void setColNameMin(Hashtable<String, String> colNameMin) {
        this.colNameMin = colNameMin;
    }

    public Hashtable<String, String> getColNameMax() {
        return colNameMax;
    }

    public void setColNameMax(Hashtable<String, String> colNameMax) {
        this.colNameMax = colNameMax;
    }

    public int getIndexCount() {
        return indexCount;
    }

    public void setIndexCount(int indexCount) {
        this.indexCount = indexCount;
    }

    public Vector<Vector<String>> getIndexName() {
        return indexName;
    }

    public void setIndexName(Vector<Vector<String>> indexName) {
        this.indexName = indexName;
    }

    public int getPages() {
        return pages;
    }

    public void setPages(int pages) {
        this.pages = pages;
    }

    public Vector getMax() {
        return max;
    }

    public void setMax(Vector max) {
        this.max = max;
    }

    public Vector<Integer> getSize() {
        return size;
    }

    public void setSize(Vector<Integer> size) {
        this.size = size;
    }

    public Vector<Integer> getHasOverflow() {
        return hasOverflow;
    }

    public void setHasOverflow(Vector<Integer> hasOverflow) {
        this.hasOverflow = hasOverflow;
    }
}
