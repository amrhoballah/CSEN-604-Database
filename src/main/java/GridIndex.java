public class GridIndex {
    private int gridNum;
    private String[] colNames;
    private double[] intervals;

    public GridIndex() {

    }
    public int getIndexOf(String colName){
        for(int i=0;i<colNames.length;i++){
            if(colNames[i].equals(colName)){
                return i;
            }
        }
        return 0;
    }

    public int getGridNum() {
        return gridNum;
    }

    public void setGridNum(int gridNum) {
        this.gridNum = gridNum;
    }

    public String[] getColNames() {
        return colNames;
    }

    public void setColNames(String[] colNames) {
        this.colNames = colNames;
    }

    public double[] getIntervals() {
        return intervals;
    }

    public void setIntervals(double[] intervals) {
        this.intervals = intervals;
    }
}
