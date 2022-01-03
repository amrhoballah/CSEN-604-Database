import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;

public class Row implements Serializable, Comparable {
    private String key;
    private Hashtable<String, Object> colNameValue;

    public Row(String keyValue, Hashtable<String, Object> colNameValue) {
        this.setColNameValue(colNameValue);
        this.setKey(keyValue);
    }

    @Override
    public int compareTo(Object o) {
        Row row = (Row) o;
        String type = String.valueOf(this.getColNameValue().get(this.getKey()).getClass());
        type = type.replace("class ", "");
        switch (type.toLowerCase()) {
            case "java.lang.integer":
                return ((Integer) Integer.parseInt(this.getColNameValue().get(this.getKey()) + ""))
                        .compareTo(Integer.parseInt(row.getColNameValue().get(row.getKey()) + ""));
            case "java.lang.double":
                return ((Double) Double.parseDouble(this.getColNameValue().get(this.getKey()) + ""))
                        .compareTo(Double.parseDouble(row.getColNameValue().get(row.getKey()) + ""));
            case "java.util.date":
                Date date1 = (Date) this.getColNameValue().get(this.getKey());
                Date date2 = (Date) row.getColNameValue().get(row.getKey());
                return date1.compareTo(date2);
            case "java.lang.string":
                return ((String) this.getColNameValue().get(this.getKey()))
                        .compareTo((String) row.getColNameValue().get(row.getKey()));
            case "java.lang.boolean":
                return ((Boolean) Boolean.parseBoolean((String) this.getColNameValue().get(this.getKey())))
                        .compareTo(Boolean.parseBoolean((String) row.getColNameValue().get(row.getKey())));
            default:
                return 0;
        }
    }

    @Override
    public String toString() {
        return getColNameValue().toString();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Hashtable<String, Object> getColNameValue() {
        return colNameValue;
    }

    public void setColNameValue(Hashtable<String, Object> colNameValue) {
        this.colNameValue = colNameValue;
    }
}
