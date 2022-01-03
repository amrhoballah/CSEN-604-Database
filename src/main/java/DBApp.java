import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    static int MaximumRowsCountinPage;
    static int MaximumKeysCountinIndexBucket;

    @Override
    public void init() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        MaximumRowsCountinPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        MaximumKeysCountinIndexBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

        File theDir = new File("src/main/resources/data");
        if (!theDir.exists()) {
            theDir.mkdirs();
        }
    }

    @Override // Done
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (List<String> record : records)
            if (record.contains(tableName))
                throw new DBAppException("Table Already Exists");
        TableData tableData = deserializeData(tableName);
        tableData.setTableName(tableName);
        for (String key : colNameType.keySet()) {
            List<String> col = new ArrayList<>();
            col.add(tableName);
            col.add(key);
            col.add(colNameType.get(key));
            col.add(key.equals(clusteringKey) ? "True" : "False");
            col.add("False");
            col.add(colNameMin.get(key));
            col.add(colNameMax.get(key));
            records.add(col);
        }


        FileWriter csvWriter;
        try {
            csvWriter = new FileWriter("src/main/resources/metadata.csv");
            for (List<String> row : records) {
                for (int i = 0; i < row.size(); i++) {
                    if (i == row.size() - 1)
                        csvWriter.append(row.get(i));
                    else
                        csvWriter.append(row.get(i)).append(",");
                }
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        serializeData(tableData);
    }

    @Override // Done
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        if (!tableExists(tableName))
            throw new DBAppException("Table " + tableName + "Does Not Exist");
        TableData tableData = deserializeData(tableName);
        for (String key : columnNames) {
            if (!(tableData.getColNameType().containsKey(key)))
                throw new DBAppException("One Or More Columns Provided Do Not Exist In The Table");
        }
        ArrayList<String> names = new ArrayList<>();
        Collections.addAll(names, columnNames);
        for (int j = 0; j < tableData.getIndexCount(); j++) {
            boolean flag = true;
            for (String key : tableData.getIndexName().get(j)) {
                if (!(names.contains(key)) || names.size() > tableData.getIndexName().get(j).size()) {
                    flag = false;
                    break;
                }
            }
            if (flag)
                throw new DBAppException("This Index Already Exists");
        }
        editCSV(tableName, columnNames);
        Vector<String> temp = new Vector<>();
        Collections.addAll(temp, columnNames);
        tableData.getIndexName().add(temp);
        GridIndex gridIndex = new GridIndex();
        gridIndex.setColNames(columnNames);
        gridIndex.setIntervals(new double[columnNames.length]);
        int k = 0;
        for (String key : columnNames) {
            double interval = 0;
            label: switch (tableData.getColNameType().get(key).toLowerCase()) {
                case "java.lang.integer":
                    interval = Integer.parseInt(tableData.getColNameMax().get(key))
                            - Integer.parseInt(tableData.getColNameMin().get(key));
                    break;
                case "java.lang.double":
                    interval = Double.parseDouble(tableData.getColNameMax().get(key))
                            - Double.parseDouble(tableData.getColNameMin().get(key));
                    break;
                case "java.util.date":
                    interval = Integer.parseInt(tableData.getColNameMax().get(key).substring(0, 4))
                            - Integer.parseInt(tableData.getColNameMin().get(key).substring(0, 4));
                    break;
                case "java.lang.string":
                    switch (tableData.getColNameMin().get(key)) {
                        case "AAAAAA":
                            interval = 52;
                            break label;
                        case "43-0000":
                            interval = 56;
                            break;
                        case "0000":
                            interval = 9999;
                            break;
                        default:
                            interval = tableData.getColNameMax().get(key).hashCode()
                                    - tableData.getColNameMin().get(key).hashCode();
                            break;
                    }
            }
            gridIndex.getIntervals()[k++] = interval / 10;
        }
        gridIndex.setGridNum(tableData.getIndexCount());
        for (int page = 0; page < tableData.getPages(); page++) {
            indexing(temp, tableName, page, tableData, gridIndex);
        }

        serializeGrid(gridIndex, tableName);
        tableData.setIndexCount(tableData.getIndexCount() + 1);
        serializeData(tableData);

    }
    private void indexing(Vector<String> columnNames, String tableName, int page, TableData tableData, GridIndex gridIndex) {
        int[] positions = new int[columnNames.size()];
        Table table = deserialize(tableName, page);
        String fileName = tableName;
        try {
            for (int i = 0; i < tableData.getHasOverflow().get(page); i++) {
                fileName += "Over";
                table.getTuples().addAll(deserialize(fileName, page).getTuples());
            }
        } catch (IndexOutOfBoundsException e) {
            e.toString();
        }
        Vector<Row> rows = table.getTuples();
        for (Row row : rows) {
            int pos = 0;
            for (String key : columnNames) {
                switch (tableData.getColNameType().get(key).toLowerCase()) {
                    case "java.lang.integer":
                        positions[pos] = (int) (((Integer.parseInt(row.getColNameValue().get(key) + "")
                                - Integer.parseInt(tableData.getColNameMin().get(key))))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.lang.double":
                        positions[pos] = (int) (((Double.parseDouble(row.getColNameValue().get(key) + "")
                                - Double.parseDouble(tableData.getColNameMin().get(key))))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.util.date":
                        positions[pos] = (int) ((((Date) row.getColNameValue().get(key)).getYear() + 1900
                                - Integer.parseInt(tableData.getColNameMin().get(key).substring(0, 4)))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.lang.string":
                        switch (tableData.getColNameMin().get(key)) {
                            case "AAAAAA":
                                String text = row.getColNameValue().get(key).toString().substring(0, 1);
                                if (text.hashCode() > 90)
                                    positions[pos] = (int) (((text.hashCode() - 6) - 65)
                                            / gridIndex.getIntervals()[pos]);
                                else
                                    positions[pos] = (int) ((text.hashCode() - 65) / gridIndex.getIntervals()[pos]);
                                break;
                            case "43-0000":
                                positions[pos] = (int) (((Integer
                                        .parseInt(row.getColNameValue().get(key).toString().substring(0, 2))) - 43)
                                        / gridIndex.getIntervals()[pos]);
                                break;
                            case "0000":
                                positions[pos] = (int) (((Integer.parseInt(row.getColNameValue().get(key).toString())))
                                        / gridIndex.getIntervals()[pos]);
                                break;
                            default:
                                positions[pos] = (int) ((row.getColNameValue().get(key).hashCode()
                                        - tableData.getColNameMin().get(key).hashCode())
                                        / gridIndex.getIntervals()[pos]);
                                break;
                        }
                        break;
                    default:
                        positions[pos] = 0;
                }
                if (positions[pos] == 10)
                    positions[pos]--;
                pos++;
            }
            StringBuilder bucketName = new StringBuilder(tableName + "Index" + gridIndex.getGridNum());
            for (int position : positions) {
                bucketName.append(position);
            }
            Bucket bucket = deserializeBucket(bucketName.toString(), gridIndex);
            fileName = bucketName.toString();
            try {
                for (int i = 0; i < 2; i++) {
                    fileName += "Over";
                    Bucket temp = deserializeBucket(fileName, gridIndex);
                    if (temp != null && bucket != null) {
                        bucket.getColNameValue().addAll(temp.getColNameValue());
                        bucket.getReferences().addAll(temp.getReferences());
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                e.toString();
            }
            int in = 0;
            if (bucket != null) {
                int ind = Collections.binarySearch((Vector) bucket.getColNameValue(), row);
                if (ind != 0) {
                    in = (ind * -1) - 1;
                }
            } else {
                bucket = new Bucket();
                bucket.setName(bucketName.toString());
            }
            if (in >= 0) {
                bucket.getColNameValue().insertElementAt(row, in);
                bucket.getReferences().insertElementAt(page, in);
            }
            bucket.setName(bucketName.toString());
            writeToBucket(bucket, gridIndex);
        }
    }
    private void deleteIndexReference(Row row, Vector<String> columnNames, TableData tableData, GridIndex gridIndex) {
        int[] positions = new int[columnNames.size()];
        int pos = 0;
        for (String key : columnNames) {
            switch (tableData.getColNameType().get(key).toLowerCase()) {
                case "java.lang.integer":
                    positions[pos] = (int) (((Integer.parseInt(row.getColNameValue().get(key) + "")
                            - Integer.parseInt(tableData.getColNameMin().get(key)))) / gridIndex.getIntervals()[pos]);
                    break;
                case "java.lang.double":
                    positions[pos] = (int) (((Double.parseDouble(row.getColNameValue().get(key) + "")
                            - Double.parseDouble(tableData.getColNameMin().get(key)))) / gridIndex.getIntervals()[pos]);
                    break;
                case "java.util.date":
                    positions[pos] = (int) ((((Date) row.getColNameValue().get(key)).getYear() + 1900
                            - Integer.parseInt(tableData.getColNameMin().get(key).substring(0, 4)))
                            / gridIndex.getIntervals()[pos]);
                    break;
                case "java.lang.string":
                    switch (tableData.getColNameMin().get(key)) {
                        case "AAAAAA":
                            String text = row.getColNameValue().get(key).toString().substring(0, 1);
                            if (text.hashCode() > 90)
                                positions[pos] = (int) (((text.hashCode() - 6) - 65) / gridIndex.getIntervals()[pos]);
                            else
                                positions[pos] = (int) ((text.hashCode() - 65) / gridIndex.getIntervals()[pos]);
                            break;
                        case "43-0000":
                            positions[pos] = (int) (((Integer
                                    .parseInt(row.getColNameValue().get(key).toString().substring(0, 2))) - 43)
                                    / gridIndex.getIntervals()[pos]);
                            break;
                        case "0000":
                            positions[pos] = (int) (((Integer.parseInt(row.getColNameValue().get(key).toString())))
                                    / gridIndex.getIntervals()[pos]);
                            break;
                        default:
                            positions[pos] = (int) ((row.getColNameValue().get(key).hashCode()
                                    - tableData.getColNameMin().get(key).hashCode()) / gridIndex.getIntervals()[pos]);
                            break;
                    }
                    break;
                default:
                    positions[pos] = 0;
            }
            if (positions[pos] == 10)
                positions[pos]--;
            pos++;
        }
        StringBuilder bucketName = new StringBuilder(tableData.getTableName() + "Index" + gridIndex.getGridNum());
        for (int position : positions) {
            bucketName.append(position);
        }
        Bucket bucket = deserializeBucket(bucketName.toString(), gridIndex);
        String fileName = bucketName.toString();
        try {
            for (int i = 0; i < 2; i++) {
                fileName += "Over";
                Bucket temp = deserializeBucket(fileName, gridIndex);
                if (temp != null && bucket != null) {
                    bucket.getColNameValue().addAll(temp.getColNameValue());
                    bucket.getReferences().addAll(temp.getReferences());
                }
            }
        } catch (IndexOutOfBoundsException e) {
            e.toString();
        }
        int deleteNum = Collections.binarySearch((Vector) bucket.getColNameValue(), row);
        bucket.getColNameValue().remove(deleteNum);
        bucket.getReferences().remove(deleteNum);
        writeToBucket(bucket, gridIndex);
    }

    @Override // Done
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (!tableExists(tableName))
            throw new DBAppException("Table " + tableName + " Does Not Exist");
        TableData tableData = deserializeData(tableName);
        for (String key : colNameValue.keySet()) {
            if (!(tableData.getColNameType().containsKey(key)))
                throw new DBAppException("One Or More Columns Provided Do Not Exist In The Table");
        }
        for (String key : colNameValue.keySet()) {
            String type = String.valueOf(colNameValue.get(key).getClass());
            type = type.replace("class ", "");
            int fail;
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(colNameValue.get(key) + ""),
                            Integer.parseInt(tableData.getColNameMax().get(key) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(colNameValue.get(key) + ""),
                            Double.parseDouble(tableData.getColNameMax().get(key) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) colNameValue.get(key);
                    Date date2 = null;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd").parse(tableData.getColNameMax().get(key));
                    } catch (ParseException e) {
                        e.toString();
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail = Math.max(((String) colNameValue.get(key)).compareTo(tableData.getColNameMax().get(key)), ((String) colNameValue.get(key)).length() - tableData.getColNameMax().get(key).length());
                    break;
                default:
                    fail = 0;
            }
            if (fail > 0)
                throw new DBAppException("One or more values are greater than the maximum allowed");
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(colNameValue.get(key) + ""),
                            Integer.parseInt(tableData.getColNameMin().get(key) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(colNameValue.get(key) + ""),
                            Double.parseDouble(tableData.getColNameMin().get(key) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) colNameValue.get(key);
                    Date date2 = null;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd").parse(tableData.getColNameMin().get(key));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail = Math.min(((String) colNameValue.get(key)).compareTo(tableData.getColNameMin().get(key)),((String) colNameValue.get(key)).length() - tableData.getColNameMin().get(key).length());
                    break;
                default:
                    fail = 0;
            }
            if (fail < 0)
                throw new DBAppException("One or more values are not within the allowed range");
        }
        for (String key : colNameValue.keySet()) {
            String inClass = colNameValue.get(key).getClass().toString();
            inClass = inClass.replace("class ", "");
            String metaClass = tableData.getColNameType().get(key);
            if (!(inClass.compareToIgnoreCase(metaClass) == 0)) {
                throw new DBAppException("Invalid Data Types");
            }
        }
        Row newRow = new Row(tableData.getKey(), colNameValue);
        int search;
        int page;
        switch (tableData.getColNameType().get(tableData.getKey()).toLowerCase()) {
            case "java.lang.integer":
                search = (Collections.binarySearch(tableData.getMax(),
                        Integer.parseInt(colNameValue.get(tableData.getKey()) + "")));
                page = search < 0 ? (search * (-1)) - 1 : search;
                break;
            case "java.lang.double":
                search = Collections.binarySearch(tableData.getMax(),
                        Double.parseDouble(colNameValue.get(tableData.getKey()) + ""));
                page = search < 0 ? (search * (-1)) - 1 : search;
                break;
            case "java.util.date":
                Date date = (Date) colNameValue.get(tableData.getKey());
                search = (Collections.binarySearch(tableData.getMax(), date));
                page = search < 0 ? ((search * (-1)) - 1) : search;
                break;
            case "java.lang.string":
                search = Collections.binarySearch(tableData.getMax(), (String) colNameValue.get(tableData.getKey()));
                page = search < 0 ? ((search * (-1)) - 1) : search;
                break;
            default:
                page = 0;
        }

        boolean shift = false;

        if (tableData.getPages() != 0 && tableData.getPages() == page
                && tableData.getSize().get(page - 1) < MaximumRowsCountinPage)
            page--;
        else if (((tableData.getPages() > page + 1 && tableData.getSize().get(page + 1) < MaximumRowsCountinPage)
                || (tableData.getPages() == page + 1 && tableData.getPages() != 0))
                && tableData.getSize().get(page) >= MaximumRowsCountinPage)
            shift = true;

        Table table = deserialize(tableName, page);
        String fileName = tableName;
        try {
            for (int i = 0; i < (!tableData.getHasOverflow().get(page).equals(null)
                    ? tableData.getHasOverflow().get(page)
                    : 0); i++) {
                fileName += "Over";
                table.getTuples().addAll(deserialize(fileName, page).getTuples());
            }
        } catch (IndexOutOfBoundsException e) {
            e.toString();
        }
        Vector<Row> rows = table.getTuples();

        boolean duplicate = false;
        for (Row tuple : rows) {
            if (tuple.getColNameValue().get(tableData.getKey()).equals(colNameValue.get(tableData.getKey()))) {
                duplicate = true;
            }
        }
        Row shifted = null;
        if (shift) {
            shifted = rows.lastElement();
            for (int i = 0; i < tableData.getIndexCount(); i++) {
                deleteIndexReference(shifted, tableData.getIndexName().get(i), tableData,
                        deserializeGrid(i, tableName));
            }
            rows.remove(rows.lastElement());
        }

        Vector searchable = table.getTuples();
        int insertPos = (Collections.binarySearch(searchable, newRow) * -1) - 1;
        if (!duplicate) {
            if (rows.size() == 0) {
                tableData.setPages(tableData.getPages() + 1);
                tableData.getHasOverflow().add(0);
                tableData.getSize().add(1);
                rows.add(newRow);
            } else {
                rows.insertElementAt(newRow, insertPos);
                tableData.getSize().set(page, rows.size());
                tableData.getHasOverflow().set(page,
                        ((int) Math.ceil((1.5 + rows.size() - 1.5) / MaximumRowsCountinPage)) - 1);
            }
        }
        if (tableData.getMax().size() > page)
            tableData.getMax().set(page, rows.lastElement().getColNameValue().get(tableData.getKey()));
        else
            tableData.getMax().add(rows.lastElement().getColNameValue().get(tableData.getKey()));

        writeToFile(page, rows, tableName);
        tableData.getSize().set(page, table.getTuples().size());
        serializeData(tableData);
        if (duplicate)
            throw new DBAppException("The record already exists");
        for (int i = 0; i < tableData.getIndexCount(); i++) {
            Vector<String> columnName = tableData.getIndexName().get(i);
            GridIndex gridIndex = deserializeGrid(i, tableName);
            indexing(columnName, tableName, page, tableData, gridIndex);
        }
        if (shift)
            insertIntoTable(tableName, shifted.getColNameValue());
    }

    @Override // Done
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (!tableExists(tableName))
            throw new DBAppException("Table " + tableName + "Does Not Exist");
        TableData tableData = deserializeData(tableName);
        for (String key : columnNameValue.keySet()) {
            if (!(tableData.getColNameType().containsKey(key)))
                throw new DBAppException("One Or More Columns Provided Do Not Exist In The Table");
        }
        boolean indexExists = false;
        int pos = 0;
        for (Vector<String> index : tableData.getIndexName()) {
            if (index.contains(tableData.getKey())) {
                indexExists = true;
                break;
            }
            pos++;
        }
        int page = 0;
        if (indexExists) {
            try {
                page = updateTableIndex(pos, tableData, clusteringKeyValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else
            page = updateTableNoIndex(tableData, clusteringKeyValue);

        Table table = deserialize(tableData.getTableName(), page);
        String fileName = tableData.getTableName();
        try {
            for (int i = 0; i < (!tableData.getHasOverflow().get(page).equals(null)
                    ? tableData.getHasOverflow().get(page)
                    : 0); i++) {
                fileName += "Over";
                table.getTuples().addAll(deserialize(fileName, page).getTuples());
            }
        } catch (IndexOutOfBoundsException e) {
            e.toString();
        }
        Vector rows = table.getTuples();
        Hashtable comp = new Hashtable();
        if (tableData.getColNameType().get(tableData.getKey()).equalsIgnoreCase("java.util.date")) {
            try {
                comp.put(tableData.getKey(), new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else
            comp.put(tableData.getKey(), clusteringKeyValue);
        int index = Collections.binarySearch(rows, new Row(tableData.getKey(), comp));

        if (index < 0)
            throw new DBAppException("No record was found");
        Row originalHash = table.getTuples().get(index);
        Row deletableHash = new Row(originalHash.getKey(),
                (Hashtable<String, Object>) originalHash.getColNameValue().clone());
        for (String key : columnNameValue.keySet()) {
            String type = String.valueOf(columnNameValue.get(key).getClass());
            type = type.replace("class ", "");
            int fail;
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(columnNameValue.get(key) + ""),
                            Integer.parseInt(tableData.getColNameMax().get(key) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(columnNameValue.get(key) + ""),
                            Double.parseDouble(tableData.getColNameMax().get(key) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) columnNameValue.get(key);
                    Date date2;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd").parse(tableData.getColNameMax().get(key));
                    } catch (ParseException e) {
                        throw new DBAppException("Unsupported Date Format");
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail = Math.max(((String) columnNameValue.get(key)).compareTo(tableData.getColNameMax().get(key)), ((String) columnNameValue.get(key)).length() - tableData.getColNameMax().get(key).length());
                    break;
                default:
                    fail = 0;
            }
            if (fail > 0)
                throw new DBAppException("One or more values are not within the allowed range");
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(columnNameValue.get(key) + ""),
                            Integer.parseInt(tableData.getColNameMin().get(key) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(columnNameValue.get(key) + ""),
                            Double.parseDouble(tableData.getColNameMin().get(key) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) columnNameValue.get(key);
                    Date date2;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd").parse(tableData.getColNameMin().get(key));
                    } catch (ParseException e) {
                        throw new DBAppException("Unsupported Date Format");
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail = Math.min(((String) columnNameValue.get(key)).compareTo(tableData.getColNameMin().get(key)),((String) columnNameValue.get(key)).length() - tableData.getColNameMin().get(key).length());
                    break;
                default:
                    fail = 0;
            }
            if (fail < 0)
                throw new DBAppException("One or more values are not within the allowed range");
        }
        for (String key : columnNameValue.keySet()) {
            Object value = columnNameValue.get(key);
            originalHash.getColNameValue().replace(key, value);
        }
        writeToFile(page, rows, tableData.getTableName());

        for (int i = 0; i < tableData.getIndexCount(); i++) {
            if (!Collections.disjoint(columnNameValue.keySet(), tableData.getIndexName().get(i))) {
                deleteIndexReference(deletableHash, tableData.getIndexName().get(i), tableData,
                        deserializeGrid(i, tableName));
                indexing(tableData.getIndexName().get(i), tableName, page, tableData, deserializeGrid(i, tableName));
            }
        }
    }
    public int updateTableIndex(int indexNum, TableData tableData, String clusteringKeyValue) throws ParseException, DBAppException {
        String key = tableData.getKey();
        GridIndex gridIndex = deserializeGrid(indexNum, tableData.getTableName());
        int pos = gridIndex.getIndexOf(tableData.getKey());
        int indexPosition;
        switch (tableData.getColNameType().get(key).toLowerCase()) {
            case "java.lang.integer":
                indexPosition = (int) (((Integer.parseInt(clusteringKeyValue)
                        - Integer.parseInt(tableData.getColNameMin().get(key)))) / gridIndex.getIntervals()[pos]);
                break;
            case "java.lang.double":
                indexPosition = (int) (((Double.parseDouble(clusteringKeyValue)
                        - Double.parseDouble(tableData.getColNameMin().get(key)))) / gridIndex.getIntervals()[pos]);
                break;
            case "java.util.date":
                indexPosition = (int) (((new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue)).getYear() + 1900
                        - Integer.parseInt(tableData.getColNameMin().get(key).substring(0, 4)))
                        / gridIndex.getIntervals()[pos]);
                break;
            case "java.lang.string":
                switch (tableData.getColNameMin().get(key)) {
                    case "AAAAAA":
                        String text = clusteringKeyValue.substring(0, 1);
                        if (text.hashCode() > 90)
                            indexPosition = (int) (((text.hashCode() - 6) - 65) / gridIndex.getIntervals()[pos]);
                        else
                            indexPosition = (int) ((text.hashCode() - 65) / gridIndex.getIntervals()[pos]);
                        break;
                    case "43-0000":
                        indexPosition = (int) (((Integer.parseInt(clusteringKeyValue.substring(0, 2))) - 43)
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "0000":
                        indexPosition = (int) (((Integer.parseInt(clusteringKeyValue)))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    default:
                        indexPosition = (int) ((clusteringKeyValue.hashCode()
                                - tableData.getColNameMin().get(key).hashCode()) / gridIndex.getIntervals()[pos]);
                        break;
                }
                break;
            default:
                indexPosition = 0;
        }
        Object theKey = null;
        switch (tableData.getColNameType().get(key).toLowerCase()) {
            case "java.lang.integer":
                theKey = Integer.parseInt(clusteringKeyValue);
                break;
            case "java.lang.double":
                theKey = Double.parseDouble(clusteringKeyValue);
                break;
            case "java.util.date":
                theKey = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
                break;
            case "java.lang.string":
                theKey = clusteringKeyValue;
        }
        String bucketName = tableData.getTableName() + "Index" + indexNum;
        int counter = 0;
        int page = -1;
        outerLoop: while (counter < Math.pow(10, gridIndex.getColNames().length)) {
            String name = namingIteration(counter++, gridIndex.getColNames().length);
            name = name.substring(0, pos) + indexPosition + name.substring(pos + 1);
            String fileName = bucketName + name;
            Bucket bucket = deserializeBucket(fileName, gridIndex);
            try {
                for (int k = 0; k < 2; k++) {
                    fileName += "Over";
                    Bucket temp = deserializeBucket(fileName, gridIndex);
                    if (temp != null && bucket != null) {
                        bucket.getColNameValue().addAll(temp.getColNameValue());
                        bucket.getReferences().addAll(temp.getReferences());
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                e.toString();
            }
            if (bucket == null) {
                bucket = new Bucket();
            }
            for (int i = 0; i < bucket.getColNameValue().size(); i++) {
                if (bucket.getColNameValue().get(i).getColNameValue().get(key).equals(theKey)) {
                    page = bucket.getReferences().get(i);
                    break outerLoop;
                }
            }
        }
        if (page == -1)
            throw new DBAppException("Value does not exist in the table");

        return page;
    }
    public int updateTableNoIndex(TableData tableData, String clusteringKeyValue) throws DBAppException {
        Date date = null;
        int search;
        int page;
        switch (tableData.getColNameType().get(tableData.getKey()).toLowerCase()) {
            case "java.lang.integer":
                search = (Collections.binarySearch(tableData.getMax(), Integer.parseInt(clusteringKeyValue)));
                page = search < 0 ? (search * (-1)) - 1 : search;
                break;
            case "java.lang.double":
                search = Collections.binarySearch(tableData.getMax(), Double.parseDouble(clusteringKeyValue));
                page = search < 0 ? (search * (-1)) - 1 : search;
                break;
            case "java.util.date":
                try {
                    date = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy").parse(clusteringKeyValue);
                } catch (ParseException e) {
                    try {
                        date = new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
                    } catch (ParseException ex) {
                        e.printStackTrace();
                    }
                }
                search = (Collections.binarySearch(tableData.getMax(), date));
                page = search < 0 ? ((search * (-1)) - 1) : search;
                break;
            case "java.lang.string":
                search = Collections.binarySearch(tableData.getMax(), clusteringKeyValue);
                page = search < 0 ? ((search * (-1)) - 1) : search;
                break;
            case "java.lang.boolean":
                search = Collections.binarySearch(tableData.getMax(), Boolean.parseBoolean(clusteringKeyValue));
                page = search < 0 ? ((search * (-1)) - 1) : search;
                break;
            default:
                page = 0;
        }
        if (page == tableData.getPages()) {
            throw new DBAppException("Key Value Not Found!");
        }

        return page;

    }

    @Override // Done
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (!tableExists(tableName))
            throw new DBAppException("Table " + tableName + "Does Not Exist");
        TableData tableData = deserializeData(tableName);
        boolean empty;
        for (String key : columnNameValue.keySet()) {
            if (!(tableData.getColNameType().containsKey(key)))
                throw new DBAppException("One Or More Columns Provided Do Not Exist In The Table");
        }
        if (columnNameValue.containsKey(tableData.getKey())) {
            boolean indexExists = false;
            int pos = 0;
            for (Vector<String> index : tableData.getIndexName()) {
                if (index.contains(tableData.getKey())) {
                    indexExists = true;
                    break;
                }
                pos++;
            }
            int page = 0;
            if (indexExists) {
                try {
                    page = updateTableIndex(pos, tableData, columnNameValue.get(tableData.getKey()) + "");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else
                page = updateTableNoIndex(tableData, columnNameValue.get(tableData.getKey()) + "");

            deleteFromPage(tableName, page, tableData, columnNameValue);
            return;

        }
        boolean indexExists = false;
        int gridNum = 0;
        for (Vector<String> index : tableData.getIndexName()) {
            if (columnNameValue.keySet().containsAll(index)) {
                indexExists = true;
                break;
            }
            gridNum++;
        }
        if (indexExists) {
            int page;
            GridIndex gridIndex = deserializeGrid(gridNum, tableName);
            int[] positions = new int[tableData.getIndexName().size()];
            int pos = 0;
            Hashtable<String, Object> bucketDelete = new Hashtable();
            for (String key : tableData.getIndexName().get(gridNum)) {
                bucketDelete.put(key, columnNameValue.get(key));
                switch (tableData.getColNameType().get(key).toLowerCase()) {
                    case "java.lang.integer":
                        positions[pos] = (int) (((Integer.parseInt(columnNameValue.get(key) + "")
                                - Integer.parseInt(tableData.getColNameMin().get(key))))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.lang.double":
                        positions[pos] = (int) (((Double.parseDouble(columnNameValue.get(key) + "")
                                - Double.parseDouble(tableData.getColNameMin().get(key))))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.util.date":
                        positions[pos] = (int) ((((Date) columnNameValue.get(key)).getYear() + 1900
                                - Integer.parseInt(tableData.getColNameMin().get(key).substring(0, 4)))
                                / gridIndex.getIntervals()[pos]);
                        break;
                    case "java.lang.string":
                        switch (tableData.getColNameMin().get(key)) {
                            case "AAAAAA":
                                String text = columnNameValue.get(key).toString().substring(0, 1);
                                if (text.hashCode() > 90)
                                    positions[pos] = (int) (((text.hashCode() - 6) - 65)
                                            / gridIndex.getIntervals()[pos]);
                                else
                                    positions[pos] = (int) ((text.hashCode() - 65) / gridIndex.getIntervals()[pos]);
                                break;
                            case "43-0000":
                                positions[pos] = (int) (((Integer
                                        .parseInt(columnNameValue.get(key).toString().substring(0, 2))) - 43)
                                        / gridIndex.getIntervals()[pos]);
                                break;
                            case "0000":
                                positions[pos] = (int) (((Integer.parseInt(columnNameValue.get(key).toString())))
                                        / gridIndex.getIntervals()[pos]);
                                break;
                            default:
                                positions[pos] = (int) ((columnNameValue.get(key).hashCode()
                                        - tableData.getColNameMin().get(key).hashCode())
                                        / gridIndex.getIntervals()[pos]);
                                break;
                        }
                        break;
                    default:
                        positions[pos] = 0;
                }
                if (positions[pos] == 10)
                    positions[pos]--;
                pos++;
            }
            StringBuilder bucketName = new StringBuilder(tableData.getTableName() + "Index" + gridIndex.getGridNum());
            for (int position : positions) {
                bucketName.append(position);
            }
            Bucket bucket = deserializeBucket(bucketName.toString(), gridIndex);
            String fileName = bucketName.toString();
            try {
                for (int i = 0; i < 2; i++) {
                    fileName += "Over";
                    Bucket temp = deserializeBucket(fileName, gridIndex);
                    if (temp != null && bucket != null) {
                        bucket.getColNameValue().addAll(temp.getColNameValue());
                        bucket.getReferences().addAll(temp.getReferences());
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                e.toString();
            }
            boolean flag = true;
            for (int i = 0; i < bucket.getColNameValue().size(); i++) {
                for (String key : bucketDelete.keySet()) {
                    if (!bucket.getColNameValue().get(i).getColNameValue().get(key).equals(bucketDelete.get(key))) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    page = bucket.getReferences().get(i);
                    columnNameValue = (Hashtable<String, Object>) bucket.getColNameValue().get(i).getColNameValue()
                            .clone();
                    deleteFromPage(tableName, page, tableData, columnNameValue);
                }
            }
            return;
        }

        Table table;
        String fileName = tableName;
        boolean delete;
        boolean done = false;
        for (int i = 0; i < tableData.getPages(); i++) {
            empty = false;
            table = deserialize(tableName, i);
            try {
                for (int j = 0; j < (!tableData.getHasOverflow().get(j).equals(null) ? tableData.getHasOverflow().get(j)
                        : 0); j++) {
                    fileName += "Over";
                    table.getTuples().addAll(deserialize(fileName, j).getTuples());
                }
            } catch (IndexOutOfBoundsException e) {
                e.toString();
            }
            for (int j = 0; j < table.getTuples().size(); j++) {
                delete = false;
                Row originalHash = table.getTuples().get(j);
                for (String key : columnNameValue.keySet()) {
                    if ((columnNameValue.get(key).equals(originalHash.getColNameValue().get(key))))
                        delete = true;
                }
                if (delete) {
                    table.getTuples().remove(j);
                    tableData.getSize().set(i, tableData.getSize().get(i) - 1);
                    j--;
                    done = true;
                }
                if (table.getTuples().size() == 0) {
                    empty = true;
                    tableData.getMax().remove(i);
                    tableData.getHasOverflow().remove(i);
                    tableData.setPages(tableData.getPages() - 1);
                    tableData.getSize().remove(i);
                } else
                    tableData.getMax().set(i,
                            table.getTuples().lastElement().getColNameValue().get(tableData.getKey()));

            }
            int numOfFiles = writeToFile(i, table.getTuples(), fileName);
            if (!empty) {
                tableData.getHasOverflow().set(i, numOfFiles - 1);
            }
            fileName = tableName;
            if (empty) {
                serializeData(tableData);
                repage(tableData, i);
                i--;
            }
        }
        serializeData(tableData);
        if (!done)
            throw new DBAppException("No Record Was Found");
    }
    private void deleteFromPage(String tableName, int page, TableData tableData, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table table = deserialize(tableName, page);
        boolean empty = false;
        String fileName = tableName;
        try {
            for (int i = 0; i < (!tableData.getHasOverflow().get(page).equals(null)
                    ? tableData.getHasOverflow().get(page)
                    : 0); i++) {
                fileName += "Over";
                table.getTuples().addAll(deserialize(fileName, page).getTuples());
            }
        } catch (IndexOutOfBoundsException e) {
            e.toString();
        }
        Vector rows = table.getTuples();
        Hashtable comp = new Hashtable();

        comp.put(tableData.getKey(), columnNameValue.get(tableData.getKey()));
        int index = Collections.binarySearch(rows, new Row(tableData.getKey(), comp));
        if (index < 0)
            throw new DBAppException("No Record Was Found");

        Row specificRow = (Row) rows.get(index);
        Row deletable = new Row(specificRow.getKey() + "",
                (Hashtable<String, Object>) specificRow.getColNameValue().clone());
        boolean canDelete = true;
        for (String item : columnNameValue.keySet()) {
            if (!(columnNameValue.get(item).equals(specificRow.getColNameValue().get(item))))
                canDelete = false;
        }
        if (canDelete) {
            table.getTuples().remove(index);

        }
        for (int i = 0; i < tableData.getIndexCount(); i++) {
            deleteIndexReference(deletable, tableData.getIndexName().get(i), tableData, deserializeGrid(i, tableName));
        }
        if (table.getTuples().size() == 0) {
            empty = true;
            tableData.getMax().remove(page);
            tableData.getHasOverflow().remove(page);
            tableData.setPages(tableData.getPages() - 1);
            tableData.getSize().remove(page);
        } else
            tableData.getMax().set(page, table.getTuples().lastElement().getColNameValue().get(tableData.getKey()));
        int numOfFiles = writeToFile(page, rows, tableName);
        if (!empty) {
            tableData.getSize().set(page, table.getTuples().size());
            tableData.getHasOverflow().set(page, numOfFiles - 1);
            serializeData(tableData);
        }

        if (!canDelete)
            throw new DBAppException("No Record Was Found");
        if (empty) {
            serializeData(tableData);
            repage(tableData, page);
        }
    }

    @Override // Done
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        String tableName = sqlTerms[0]._strTableName;
        if (!tableExists(tableName))
            throw new DBAppException("Table " + tableName + "Does Not Exist");
        TableData tableData = deserializeData(tableName);
        // Start of Validation
        for (SQLTerm term : sqlTerms) {
            if (!(tableData.getColNameType().containsKey(term._strColumnName)))
                throw new DBAppException("One Or More Columns Provided Do Not Exist In The Table");
        }
        for (SQLTerm term : sqlTerms) {
            String inClass = term._objValue.getClass().toString();
            inClass = inClass.replace("class ", "");
            String metaClass = tableData.getColNameType().get(term._strColumnName);
            if (!(inClass.compareToIgnoreCase(metaClass) == 0)) {
                throw new DBAppException("Invalid Data Types");
            }
        }
        for (SQLTerm term : sqlTerms) {
            String type = String.valueOf(term._objValue.getClass());
            type = type.replace("class ", "");
            int fail;
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(term._objValue + ""),
                            Integer.parseInt(tableData.getColNameMax().get(term._strColumnName) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(term._objValue + ""),
                            Double.parseDouble(tableData.getColNameMax().get(term._strColumnName) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) term._objValue;
                    Date date2 = null;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd")
                                .parse(tableData.getColNameMax().get(term._strColumnName));
                    } catch (ParseException e) {
                        e.toString();
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail =  ((String) term._objValue).compareTo(tableData.getColNameMax().get(term._strColumnName));
                    break;
                default:
                    fail = 0;
            }
            if (fail > 0)
                throw new DBAppException("One or more values are not within the allowed range");
            switch (type.toLowerCase()) {
                case "java.lang.integer":
                    fail = Integer.compare(Integer.parseInt(term._objValue + ""),
                            Integer.parseInt(tableData.getColNameMin().get(term._strColumnName) + ""));
                    break;
                case "java.lang.double":
                    fail = Double.compare(Double.parseDouble(term._objValue + ""),
                            Double.parseDouble(tableData.getColNameMin().get(term._strColumnName) + ""));
                    break;
                case "java.util.date":
                    Date date1 = (Date) term._objValue;
                    Date date2 = null;
                    try {
                        date2 = new SimpleDateFormat("yyyy-MM-dd")
                                .parse(tableData.getColNameMin().get(term._strColumnName));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    fail = date1.compareTo(date2);
                    break;
                case "java.lang.string":
                    fail = ((String) term._objValue).compareTo(tableData.getColNameMin().get(term._strColumnName));
                    break;
                default:
                    fail = 0;
            }
            if (fail < 0)
                throw new DBAppException("One or more values are not within the allowed range");
        }
        // End of Validation

        boolean[] indexExists = new boolean[sqlTerms.length];
        int gridNum = 0;
        Vector<String> colNames = new Vector<>();
        for (SQLTerm term : sqlTerms) {
            colNames.add(term._strColumnName);
        }
        Vector<Row> maybe = new Vector<>();
        int j = 0;
        outerloop:
        for (String col : colNames) {
            gridNum = 0;
            for (Vector<String> index : tableData.getIndexName()) {
                if (index.contains(col)) {
                    indexExists[j] = true;
                    Vector<SQLTerm> temp = new Vector<>();
                    for (SQLTerm term : sqlTerms) {
                        if (index.contains(term._strColumnName)) {
                            temp.add(term);
                        }
                    }
                    maybe.addAll(selectIndex(temp, tableData, gridNum, index));
                    break outerloop;
                }
                gridNum++;
            }
            j++;
        }

        Vector list = new Vector();
        Collections.addAll(list, arrayOperators);
        boolean forAll = true;
        boolean forNone = true;
        for (boolean flag : indexExists) {
            if (!flag) {
                forAll = false;
            }
            if (flag) {
                forNone = false;
            }
        }
        if (forAll) {
            return selectIndexHelper(sqlTerms, maybe, list, tableData);
        }
        if (!forNone) {
            for (int i = 0; i < indexExists.length; i++) {
                if (!indexExists[i])
                    maybe.addAll(selectNoIndex(new SQLTerm[] { sqlTerms[i] }, tableData, new Vector<>()));
            }
            return selectIndexHelper(sqlTerms, maybe, list, tableData);
        }
        return selectNoIndex(sqlTerms, tableData, list).iterator();
    }
    private Vector<Row> selectIndex(Vector<SQLTerm> sqlTerms, TableData tableData, int gridNum, Vector<String> indexColNames) {
        GridIndex gridIndex = deserializeGrid(gridNum, tableData.getTableName());
        int[] positions = new int[sqlTerms.size()];
        int pos = 0;
        for (SQLTerm term : sqlTerms) {
            int ipos = gridIndex.getIndexOf(term._strColumnName);
            switch (tableData.getColNameType().get(term._strColumnName).toLowerCase()) {
                case "java.lang.integer":
                    positions[pos] = (int) (((Integer.parseInt(term._objValue + "")
                            - Integer.parseInt(tableData.getColNameMin().get(term._strColumnName))))
                            / gridIndex.getIntervals()[ipos]);
                    break;
                case "java.lang.double":
                    positions[pos] = (int) (((Double.parseDouble(term._objValue + "")
                            - Double.parseDouble(tableData.getColNameMin().get(term._strColumnName))))
                            / gridIndex.getIntervals()[ipos]);
                    break;
                case "java.util.date":
                    positions[pos] = (int) ((((Date) term._objValue).getYear() + 1900
                            - Integer.parseInt(tableData.getColNameMin().get(term._strColumnName).substring(0, 4)))
                            / gridIndex.getIntervals()[ipos]);
                    break;
                case "java.lang.string":
                    switch (tableData.getColNameMin().get(term._strColumnName)) {
                        case "AAAAAA":
                            String text = term._objValue.toString().substring(0, 1);
                            if (text.hashCode() > 90)
                                positions[pos] = (int) (((text.hashCode() - 6) - 65) / gridIndex.getIntervals()[ipos]);
                            else
                                positions[pos] = (int) ((text.hashCode() - 65) / gridIndex.getIntervals()[ipos]);
                            break;
                        case "43-0000":
                            positions[pos] = (int) (((Integer.parseInt(term._objValue.toString().substring(0, 2))) - 43)
                                    / gridIndex.getIntervals()[ipos]);
                            break;
                        case "0000":
                            positions[pos] = (int) (((Integer.parseInt(term._objValue.toString())))
                                    / gridIndex.getIntervals()[ipos]);
                            break;
                        default:
                            positions[pos] = (int) ((term._objValue.hashCode()
                                    - tableData.getColNameMin().get(term._strColumnName).hashCode())
                                    / gridIndex.getIntervals()[ipos]);
                            break;
                    }
                    break;
                default:
                    positions[pos] = 0;
            }
            if (positions[pos] == 10)
                positions[pos]--;
            pos++;
        }
        Vector<Row> rows = new Vector<>();
        String bucketName = tableData.getTableName() + "Index" + gridNum;
        for (int i = 0; i < sqlTerms.size(); i++) {
            SQLTerm term = sqlTerms.get(i);
            int fixed = indexColNames.indexOf(term._strColumnName);
            int start = 0;
            int end = 0;
            boolean flag = false;
            switch (term._strOperator) {
                case ">":
                case ">=":
                    start = positions[i];
                    end = 9;
                    break;
                case "<":
                case "<=":
                    end = positions[i];
                    break;
                case "!=":
                    start = 0;
                    end = 9;
                    break;
                case "=":
                    start = positions[i];
                    end = start;
                    break;
            }
            int counter = 0;
            while (counter < Math.pow(10, gridIndex.getColNames().length)) {
                String name = namingIteration(counter++, indexColNames.size());
                for (int j = start; j <= end; j++) {
                    name = name.substring(0, fixed) + j + name.substring(fixed + 1);
                    String fileName = bucketName + name;
                    Bucket bucket = deserializeBucket(fileName, gridIndex);
                    try {
                        for (int k = 0; k < 2; k++) {
                            fileName += "Over";
                            Bucket temp = deserializeBucket(fileName, gridIndex);
                            if (temp != null && bucket != null) {
                                bucket.getColNameValue().addAll(temp.getColNameValue());
                                bucket.getReferences().addAll(temp.getReferences());
                            }
                        }
                    } catch (IndexOutOfBoundsException e) {
                        e.toString();
                    }
                    if (bucket == null) {
                        bucket = new Bucket();
                    }
                    rows.addAll(bucket.getColNameValue());
                }
            }
        }
        return rows;
    }
    private Iterator selectIndexHelper(SQLTerm[] sqlTerms, Vector<Row> rows, Vector<String> arrayOperators, TableData tableData) {
        Vector<String> vector = new Vector<>();

        for (Row item : rows) {
            Vector<String> ops = (Vector<String>) arrayOperators.clone();
            boolean[] flags = new boolean[sqlTerms.length];
            for (int i = 0; i < sqlTerms.length; i++) {
                SQLTerm sqlTerm = sqlTerms[i];
                switch (tableData.getColNameType().get(sqlTerm._strColumnName).toLowerCase()) {
                    case "java.lang.integer":
                        switch (sqlTerm._strOperator) {
                            case ">":
                                if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                        .toString()) > Integer.parseInt(sqlTerm._objValue.toString()))
                                    flags[i] = true;
                                break;
                            case ">=":
                                if ((Integer
                                        .parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                .toString()) > Integer.parseInt(sqlTerm._objValue.toString())
                                        || Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                .toString()) == Integer.parseInt(sqlTerm._objValue.toString())))
                                    flags[i] = true;
                                break;
                            case "<":
                                if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                        .toString()) < Integer.parseInt(sqlTerm._objValue.toString()))
                                    flags[i] = true;
                                break;
                            case "<=":
                                if ((Integer
                                        .parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                .toString()) < Integer.parseInt(sqlTerm._objValue.toString())
                                        || Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                .toString()) == Integer.parseInt(sqlTerm._objValue.toString())))
                                    flags[i] = true;
                                break;
                            case "!=":
                                if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                        .toString()) != Integer.parseInt(sqlTerm._objValue.toString()))
                                    flags[i] = true;
                                break;
                            case "=":
                                if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                        .toString()) == Integer.parseInt(sqlTerm._objValue.toString()))
                                    flags[i] = true;
                                break;
                        }
                        break;
                    case "java.lang.double":
                        switch (sqlTerm._strOperator) {
                            case ">":
                                if (Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) > 0)
                                    flags[i] = true;
                                break;
                            case ">=":
                                if ((Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) > 0
                                        || Double.compare(
                                                Double.parseDouble(item.getColNameValue().get((sqlTerm._strColumnName))
                                                        .toString()),
                                                Double.parseDouble(sqlTerm._objValue.toString())) == 0))
                                    flags[i] = true;
                                break;
                            case "<":
                                if (Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) < 0)
                                    flags[i] = true;
                                break;
                            case "<=":
                                if ((Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) < 0
                                        || Double.compare(
                                                Double.parseDouble(item.getColNameValue().get((sqlTerm._strColumnName))
                                                        .toString()),
                                                Double.parseDouble(sqlTerm._objValue.toString())) == 0))
                                    flags[i] = true;
                                break;
                            case "!=":
                                if (Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) != 0)
                                    flags[i] = true;
                                break;
                            case "=":
                                if (Double.compare(
                                        Double.parseDouble(
                                                item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                        Double.parseDouble(sqlTerm._objValue.toString())) == 0)
                                    flags[i] = true;
                                break;
                        }
                        break;
                    case "java.util.date":
                        switch (sqlTerm._strOperator) {
                            case ">":
                                if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) > 0)
                                    flags[i] = true;
                                break;
                            case ">=":
                                if ((((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) > 0
                                        || ((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                                .compareTo((Date) sqlTerm._objValue) == 0))
                                    flags[i] = true;
                                break;
                            case "<":
                                if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) < 0)
                                    flags[i] = true;
                                break;
                            case "<=":
                                if ((((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) < 0
                                        || ((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                                .compareTo((Date) sqlTerm._objValue) == 0))
                                    flags[i] = true;
                                break;
                            case "!=":
                                if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) != 0)
                                    flags[i] = true;
                                break;
                            case "=":
                                if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                        .compareTo((Date) sqlTerm._objValue) == 0)
                                    flags[i] = true;
                                break;
                        }
                        break;
                    case "java.lang.string":
                        switch (sqlTerm._strOperator) {
                            case ">":
                                if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) > 0)
                                    flags[i] = true;
                                break;
                            case ">=":
                                if (((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) > 0
                                        || item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                                .compareTo(sqlTerm._objValue.toString()) == 0))
                                    flags[i] = true;
                                break;
                            case "<":
                                if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) < 0)
                                    flags[i] = true;
                                break;
                            case "<=":
                                if (((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) < 0
                                        || item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                                .compareTo(sqlTerm._objValue.toString()) == 0))
                                    flags[i] = true;
                                break;
                            case "!=":
                                if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) != 0)
                                    flags[i] = true;
                                break;
                            case "=":
                                if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                        .compareTo(sqlTerm._objValue.toString())) == 0)
                                    flags[i] = true;
                                break;
                        }
                        break;
                }
            }
            Vector<Boolean> results = new Vector<>();
            for (int i = 0; i < flags.length; i++) {
                results.add(flags[i]);
            }
            for(int i = 0; i < results.size()-1; i++){
                int v = 0;
                if(ops.size() > 1)
                    v = i;
                if(ops.get(v).equals("AND")){
                    results.set(i,results.get(i) & results.get(i+1));
                    results.remove(i+1);
                    if(ops.size() > 1)
                        ops.remove(v);
                    i--;
                }
            }
            for(int i = 0; i < results.size()-1; i++){
                int v = 0;
                if(ops.size() > 1)
                    v = i;
                if(ops.get(v).equals("OR")){
                    results.set(i,results.get(i) | results.get(i+1));
                    results.remove(i+1);
                    if(ops.size() > 1)
                        ops.remove(v);
                    i--;
                }
            }
            for(int i = 0; i < results.size()-1; i++){
                int v = 0;
                if(ops.size() > 1)
                    v = i;
                if(ops.get(v).equals("XOR")){
                    results.set(i,results.get(i) ^ results.get(i+1));
                    results.remove(i+1);
                    if(ops.size() > 1)
                        ops.remove(v);
                    i--;
                }
            }
            if (results.contains(true) && !vector.contains(item.toString())) {
                vector.add(item.toString());
            }
        }
        return vector.iterator();
    }
    private Vector<Row> selectNoIndex(SQLTerm[] sqlTerms, TableData tableData, Vector<String> arrayOperators) {

        Vector<Row> vector = new Vector<>();
        for (int k = 0; k < tableData.getPages(); k++) {

            Table table = deserialize(tableData.getTableName(), k);
            String fileName = tableData.getTableName();
            try {
                for (int i = 0; i < (!tableData.getHasOverflow().get(k).equals(null) ? tableData.getHasOverflow().get(k)
                        : 0); i++) {
                    fileName += "Over";
                    table.getTuples().addAll(deserialize(fileName, k).getTuples());
                }
            } catch (IndexOutOfBoundsException e) {
                e.toString();
            }
            for (int j = 0; j < table.getTuples().size(); j++) {
                Row item = table.getTuples().get(j);
                Vector<String> ops = (Vector<String>) arrayOperators.clone();
                boolean[] flags = new boolean[sqlTerms.length];
                for (int i = 0; i < sqlTerms.length; i++) {
                    SQLTerm sqlTerm = sqlTerms[i];
                    switch (tableData.getColNameType().get(sqlTerm._strColumnName).toLowerCase()) {
                        case "java.lang.integer":
                            switch (sqlTerm._strOperator) {
                                case ">":
                                    if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                            .toString()) > Integer.parseInt(sqlTerm._objValue.toString()))
                                        flags[i] = true;
                                    break;
                                case ">=":
                                    if ((Integer
                                            .parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                    .toString()) > Integer.parseInt(sqlTerm._objValue.toString())
                                            || Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                    .toString()) == Integer.parseInt(sqlTerm._objValue.toString())))
                                        flags[i] = true;
                                    break;
                                case "<":
                                    if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                            .toString()) < Integer.parseInt(sqlTerm._objValue.toString()))
                                        flags[i] = true;
                                    break;
                                case "<=":
                                    if ((Integer
                                            .parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                    .toString()) < Integer.parseInt(sqlTerm._objValue.toString())
                                            || Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                                    .toString()) == Integer.parseInt(sqlTerm._objValue.toString())))
                                        flags[i] = true;
                                    break;
                                case "!=":
                                    if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                            .toString()) != Integer.parseInt(sqlTerm._objValue.toString()))
                                        flags[i] = true;
                                    break;
                                case "=":
                                    if (Integer.parseInt(item.getColNameValue().get((sqlTerm._strColumnName))
                                            .toString()) == Integer.parseInt(sqlTerm._objValue.toString()))
                                        flags[i] = true;
                                    break;
                            }
                            break;
                        case "java.lang.double":
                            switch (sqlTerm._strOperator) {
                                case ">":
                                    if (Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) > 0)
                                        flags[i] = true;
                                    break;
                                case ">=":
                                    if ((Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) > 0
                                            || Double.compare(
                                                    Double.parseDouble(item.getColNameValue()
                                                            .get((sqlTerm._strColumnName)).toString()),
                                                    Double.parseDouble(sqlTerm._objValue.toString())) == 0))
                                        flags[i] = true;
                                    break;
                                case "<":
                                    if (Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) < 0)
                                        flags[i] = true;
                                    break;
                                case "<=":
                                    if ((Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) < 0
                                            || Double.compare(
                                                    Double.parseDouble(item.getColNameValue()
                                                            .get((sqlTerm._strColumnName)).toString()),
                                                    Double.parseDouble(sqlTerm._objValue.toString())) == 0))
                                        flags[i] = true;
                                    break;
                                case "!=":
                                    if (Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) != 0)
                                        flags[i] = true;
                                    break;
                                case "=":
                                    if (Double.compare(
                                            Double.parseDouble(
                                                    item.getColNameValue().get((sqlTerm._strColumnName)).toString()),
                                            Double.parseDouble(sqlTerm._objValue.toString())) == 0)
                                        flags[i] = true;
                                    break;
                            }
                            break;
                        case "java.util.date":
                            switch (sqlTerm._strOperator) {
                                case ">":
                                    if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) > 0)
                                        flags[i] = true;
                                    break;
                                case ">=":
                                    if ((((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) > 0
                                            || ((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                                    .compareTo((Date) sqlTerm._objValue) == 0))
                                        flags[i] = true;
                                    break;
                                case "<":
                                    if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) < 0)
                                        flags[i] = true;
                                    break;
                                case "<=":
                                    if ((((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) < 0
                                            || ((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                                    .compareTo((Date) sqlTerm._objValue) == 0))
                                        flags[i] = true;
                                    break;
                                case "!=":
                                    if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) != 0)
                                        flags[i] = true;
                                    break;
                                case "=":
                                    if (((Date) (item.getColNameValue().get((sqlTerm._strColumnName))))
                                            .compareTo((Date) sqlTerm._objValue) == 0)
                                        flags[i] = true;
                                    break;
                            }
                            break;
                        case "java.lang.string":

                            switch (sqlTerm._strOperator) {
                                case ">":
                                    if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) > 0)
                                        flags[i] = true;
                                    break;
                                case ">=":
                                    if (((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) > 0
                                            || item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                                    .compareTo(sqlTerm._objValue.toString()) == 0))
                                        flags[i] = true;
                                    break;
                                case "<":
                                    if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) < 0)
                                        flags[i] = true;
                                    break;
                                case "<=":
                                    if (((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) < 0
                                            || item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                                    .compareTo(sqlTerm._objValue.toString()) == 0))
                                        flags[i] = true;
                                    break;
                                case "!=":
                                    if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) != 0)
                                        flags[i] = true;
                                    break;
                                case "=":
                                    if ((item.getColNameValue().get(sqlTerm._strColumnName).toString()
                                            .compareTo(sqlTerm._objValue.toString())) == 0)
                                        flags[i] = true;
                                    break;
                            }
                            break;
                    }
                }

                Vector<Boolean> results = new Vector<>();
                for (int i = 0; i < flags.length; i++) {
                    results.add(flags[i]);
                }
                for(int i = 0; i < results.size()-1; i++){
                    int v = 0;
                    if(ops.size() > 1)
                        v = i;
                    if(ops.get(v).equals("AND")){
                        results.set(i,results.get(i) & results.get(i+1));
                        results.remove(i+1);
                        if(ops.size() > 1)
                            ops.remove(v);
                        i--;
                    }
                }
                for(int i = 0; i < results.size()-1; i++){
                    int v = 0;
                    if(ops.size() > 1)
                        v = i;
                    if(ops.get(v).equals("OR")){
                        results.set(i,results.get(i) | results.get(i+1));
                        results.remove(i+1);
                        if(ops.size() > 1)
                            ops.remove(v);
                        i--;
                    }
                }
                for(int i = 0; i < results.size()-1; i++){
                    int v = 0;
                    if(ops.size() > 1)
                        v = i;
                    if(ops.get(v).equals("XOR")){
                        results.set(i,results.get(i) ^ results.get(i+1));
                        results.remove(i+1);
                        if(ops.size() > 1)
                            ops.remove(v);
                        i--;
                    }
                }
                if (results.contains(true) && !vector.contains(item)) {
                    vector.add(item);
                }
            }
            writeToFile(k, table.getTuples(), tableData.getTableName());
        }
        return vector;
    }

    // Serialization
    private Table deserialize(String tableName, int page) {
        try {
            Table table = new Table(tableName.replace("Over", ""));

            File file = new File("src/main/resources/data/" + tableName + page + ".ser");
            if (!file.isFile()) {
                file.createNewFile();
            }
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + page + ".ser");
            if (fileIn.available() > 0) {
                ObjectInputStream in = new ObjectInputStream(fileIn);
                table.setTuples((Vector<Row>) in.readObject());
                in.close();
            }
            fileIn.close();
            return table;
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
            return null;
        }
    }
    private void serialize(Table table, int page) {
        try {
            File file = new File("src/main/resources/data/" + table.getTableName() + page + ".ser");

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(
                    "src/main/resources/data/" + table.getTableName() + page + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table.getTuples());
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    private Bucket deserializeBucket(String bucketName, GridIndex grid) {
        try {
            Bucket table = new Bucket();
            String dirName = bucketName.substring(0,
                    bucketName.replace("Over", "").length() - grid.getColNames().length);
            File theDir = new File("src/main/resources/data/" + dirName);
            table.setName(bucketName);
            FileInputStream fileIn = new FileInputStream(
                    "src/main/resources/data/" + dirName + "/" + bucketName + ".ser");
            if (fileIn.available() > 0) {
                ObjectInputStream in = new ObjectInputStream(fileIn);

                table.setColNameValue((Vector<Row>) in.readObject());
                table.setReferences((Vector<Integer>) in.readObject());
                in.close();
            }
            fileIn.close();
            return table;
        } catch (IOException | ClassNotFoundException i) {
            return null;
        }
    }
    private void serializeBucket(Bucket table, GridIndex grid) {
        try {
            String dirName = table.getName().substring(0,
                    table.getName().replace("Over", "").length() - grid.getColNames().length);
            File theDir = new File("src/main/resources/data/" + dirName);
            if (!theDir.exists()) {
                theDir.mkdirs();
            }
            File file = new File("src/main/resources/data/" + dirName + "/" + table.getName() + ".ser");

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(
                    "src/main/resources/data/" + dirName + "/" + table.getName() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table.getColNameValue());
            out.writeObject(table.getReferences());
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    private TableData deserializeData(String tableName) {
        try {
            File file = new File("src/main/resources/data/" + tableName + "Data.ser");
            if (!file.isFile()) {
                file.createNewFile();
            }
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "Data.ser");
            TableData tableData = new TableData();

            tableData.setTableName(tableName);
            if (fileIn.available() > 0) {
                ObjectInputStream in = new ObjectInputStream(fileIn);
                tableData.setMax((Vector) in.readObject());
                tableData.setPages((Integer) in.readObject());
                tableData.setSize((Vector<Integer>) in.readObject());
                tableData.setHasOverflow((Vector<Integer>) in.readObject());
                tableData.setIndexCount((Integer) in.readObject());
                tableData.setIndexName((Vector<Vector<String>>) in.readObject());
                in.close();
            }
            List<List<String>> columnsMeta = readCsv(tableName);

            Hashtable<String, String> colNameType = new Hashtable<>();
            Hashtable<String, String> colNameMin = new Hashtable<>();
            Hashtable<String, String> colNameMax = new Hashtable<>();

            for (List<String> column : columnsMeta) {
                colNameType.put(column.get(1).trim(), column.get(2).trim());
                colNameMin.put(column.get(1).trim(), column.get(5).trim());
                colNameMax.put(column.get(1).trim(), column.get(6).trim());
                if (column.get(3).trim().equals("True")) {
                    tableData.setKey(column.get(1).trim());
                }
            }
            tableData.setColNameMin(colNameMin);
            tableData.setColNameMax(colNameMax);
            tableData.setColNameType(colNameType);
            fileIn.close();
            return tableData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    private void serializeData(TableData tableData) {
        try {
            File file = new File("src/main/resources/data/" + tableData.getTableName() + "Data.ser");

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(
                    "src/main/resources/data/" + tableData.getTableName() + "Data.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tableData.getMax());
            out.writeObject(tableData.getPages());
            out.writeObject(tableData.getSize());
            out.writeObject(tableData.getHasOverflow());
            out.writeObject(tableData.getIndexCount());
            out.writeObject(tableData.getIndexName());
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    private GridIndex deserializeGrid(int gridNum, String tableName) {
        try {
            File file = new File("src/main/resources/data/" + tableName + gridNum + "Grid.ser");
            if (!file.isFile()) {
                file.createNewFile();
            }
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + gridNum + "Grid.ser");
            GridIndex tableData = new GridIndex();
            tableData.setGridNum(gridNum);
            if (fileIn.available() > 0) {
                ObjectInputStream in = new ObjectInputStream(fileIn);
                tableData.setColNames((String[]) in.readObject());
                tableData.setIntervals((double[]) in.readObject());

                in.close();
            }

            return tableData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    private void serializeGrid(GridIndex tableData, String tableName) {
        try {
            File file = new File("src/main/resources/data/" + tableName + tableData.getGridNum() + "Grid.ser");

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(
                    "src/main/resources/data/" + tableName + tableData.getGridNum() + "Grid.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tableData.getColNames());
            out.writeObject(tableData.getIntervals());

            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    private int writeToFile(int page, Vector<Row> rows, String fileName) {
        int numOfFiles = (int) Math.ceil((1.5 + rows.size() - 1.5) / MaximumRowsCountinPage);
        if (numOfFiles > 4)
            return 0;
        int start = 0;
        int end = Math.min(rows.size(), MaximumRowsCountinPage);
        for (int i = 0; i < numOfFiles; i++) {
            if (i > 0)
                fileName += "Over";
            Table tempTable = new Table(fileName);
            tempTable.getTuples().addAll(rows.subList(start, end));
            serialize(tempTable, page);
            start = end;
            end = Math.min(rows.size(), MaximumRowsCountinPage + end);
        }
        return numOfFiles;
    }
    private void writeToBucket(Bucket bucket, GridIndex gridIndex) {
        int numOfFiles = (int) Math.ceil((1.5 + bucket.getReferences().size() - 1.5) / MaximumKeysCountinIndexBucket);
        int start = 0;
        int end = Math.min(bucket.getReferences().size(), MaximumKeysCountinIndexBucket);
        for (int i = 0; i < numOfFiles; i++) {
            if (i > 0)
                bucket.setName(bucket.getName() + "Over");
            Bucket tempTable = new Bucket();
            tempTable.setName(bucket.getName());
            tempTable.getColNameValue().addAll(bucket.getColNameValue().subList(start, end));
            tempTable.getReferences().addAll(bucket.getReferences().subList(start, end));
            serializeBucket(tempTable, gridIndex);
            start = end;
            end = Math.min(bucket.getReferences().size(), MaximumKeysCountinIndexBucket + end);
        }
    }

    // Page Renaming On Delete
    private void repage(TableData tableData, int page) {
        for (int i = page + 1; i < tableData.getPages() + 1; i++) {
            String filename = tableData.getTableName();
            for (int j = 0; j < tableData.getHasOverflow().get(i - 1) + 1; j++) {
                if (j > 0)
                    filename += "Over";
                Table tempTable = deserialize(filename, i);
                tempTable.setTableName(filename);
                serialize(tempTable, i - 1);
                File file = new File("src/main/resources/data/" + filename + i + ".ser");
                file.delete();
            }
        }
    }

    // CSV
    private List<List<String>> readCsv(String tableName) {
        List<List<String>> columnsMeta = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values[0].equals(tableName.replace("Over", ""))) {
                    columnsMeta.add(Arrays.asList(values));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return columnsMeta;
    }
    private void editCSV(String tableName, String[] columnNames) {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (List<String> record : records) {
            for (String col : columnNames) {
                if (record.contains(tableName) && record.contains(col)) {
                    record.set(4, "True");
                }
            }
        }
        FileWriter csvWriter;
        try {
            csvWriter = new FileWriter("src/main/resources/metadata.csv");
            for (List<String> row : records) {
                for (int i = 0; i < row.size(); i++) {
                    if (i == row.size() - 1)
                        csvWriter.append(row.get(i));
                    else
                        csvWriter.append(row.get(i)).append(",");
                }
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // File Name Controller
    private String namingIteration(int counter, int length) {
        int len = (counter + "").length();
        String newNumbers = "0".repeat(Math.max(0, length - len)) + counter;
        return newNumbers;
    }
    private boolean tableExists(String tableName) {
        if (readCsv(tableName).isEmpty())
            return false;
        return true;
    }
}