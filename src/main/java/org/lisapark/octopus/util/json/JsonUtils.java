/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.octopus.util.json;

//import com.mongodb.DB;
//import com.mongodb.DBCollection;
//import com.mongodb.DBObject;
//import com.mongodb.Mongo;
//import com.mongodb.util.JSON;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public abstract class JsonUtils {
    
    public static final String HOST                 = "host";
    public static final String PORT                 = "port";
    public static final String CLASS_NAME           = "classname";
    public static final String URL                  = "url";
    public static final String USER_NAME            = "username";
    public static final String PASSWORD             = "password";
    public static final String SQL                  = "sql";
    public static final String FILE_UUID            = "fileuuid";
    public static final String FILE_NAME            = "filename";
    public static final String MONGO_DB_NAME        = "mongodbname";
    public static final String MONGO_COLLECTION     = "mongocollection";
    
    public static final String FILE_ID_QUERY        = "SELECT CONTENT FROM REPOSITORY WHERE ID = ?";
    public static final String FILE_NAME_QUERY      = "SELECT CONTENT FROM REPOSITORY WHERE NAME = ?";
    
    public static final String SPREAD_SHEET_ROW     = "row";
    public static final String SPREAD_SHEET_ROWS    = "rows";
    public static final String DEFAULT_NODE_NAME    = "row";
        
    public static void main(String[] args){
        
        String fileName = "C:/Users/Alex/Documents/NetBeansProjects/FROM_NJ_SVN/octopus-1.0-dist/octopus-designer/bin/TestExcel_001.xls";
        
        JsonUtils utils = new JsonUtils() {
            @Override
            public List<String> dataFieldNames() {
                List<String> list = new ArrayList<String>();
                
                list.add("name");
                list.add("field2");
                list.add("fiels3");
                list.add("field4");
                list.add("category");
                list.add("resource_qty");
                list.add("product_qty");
                
                return list;
            }

            @Override
            public List<String> treeNodeNames() {
                List<String> list = new ArrayList<String>();
                
                list.add("shop");
                list.add("date shift");
//                list.add("shift");
                list.add("machine");
                list.add("productname");
                list.add("producttype");
                list.add("itemname");
                
                return list;
            }
        };
        
        Properties props = new Properties();
        props.put(HOST, "localhost");
        props.put(PORT, "27017");
        props.put(MONGO_DB_NAME, "octopus");
        props.put(MONGO_COLLECTION, "octopus");
        
        try {
            
            HSSFWorkbook book = ExcelUtils.excelWorkbookFromFile(fileName);
            String json = utils.jsonStringFromSSbyIndex(book, 0);
            
            System.out.println(json);
            
//            DBCollection coll = mongoCollection(props);
//            DBObject dbObject = (DBObject) JSON.parse(json);            
           
//            coll.save(dbObject);
            
        } catch (JSONException ex) {
            Exceptions.printStackTrace(ex);
        } catch (FileNotFoundException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }
        
    }
    
    private static final String REPORT              = "Отчет";
    
    public String jsonStringFromSSbyIndex(HSSFWorkbook workbook, int index) 
            throws JSONException, FileNotFoundException, IOException {
        // Get Sheet by index.
        if (workbook.getNumberOfSheets() > index) {
            Sheet sheet = workbook.getSheetAt(index);
            return jsonFromSS(sheet, 2);
        } else {
            return null;
        }
    }
    
    public String jsonStringFromSSbyName(HSSFWorkbook workbook, String name) 
            throws JSONException, FileNotFoundException, IOException {
        // Get Sheet by index.
        Sheet sheet = workbook.getSheet(name);
        return jsonFromSS(sheet, 2);
    }

    /**
     * 
     * @param sheet
     * @param ontology
     * @return
     * @throws JSONException 
     */
    private String jsonFromSS(Sheet sheet, int increment) throws JSONException {
        // Return null, if sheet is null
        if(sheet == null) return null;
        
        String sheetName = sheet.getSheetName();
        if(sheetName.isEmpty()){
            sheetName = SPREAD_SHEET_ROWS;
        }        
             
        // Iterate through the rows.
        StringBuilder stringBuilderRows = new StringBuilder();        
        List<String> stack = new ArrayList<String>();
        Boolean first = Boolean.TRUE;        
        int splitRowNumber = 0;        
        if (sheet.getPaneInformation() != null && sheet.getPaneInformation().isFreezePane()) {
            splitRowNumber = sheet.getPaneInformation().getHorizontalSplitPosition();
        }        
        for (Iterator<Row> rowsIT = sheet.rowIterator(); rowsIT.hasNext();) {
            Row row = rowsIT.next();
            if (row.getPhysicalNumberOfCells() > 0 && row.getRowNum() >= splitRowNumber) {
                continue;
            }
            String stringCells = jsonFromRowAsString(row);
            if (stringCells.isEmpty()) {
                continue;
            }

            String stringRow = jsonFromRowAsTreeNode(row, stringCells, stack, increment);

            if (first) {
                stringBuilderRows.append("[").append(stringRow);
                first = Boolean.FALSE;
            } else {
                stringBuilderRows.append(",").append(stringRow);
            }

        }      
        // Get the JSON text.
        stringBuilderRows = stringBuilderRows.append("]"
//                + "}"
                );
        
        return //"{" + "\"" + sheetName + "\"" + " : " + 
                stringBuilderRows.toString();
    }

    
    private String jsonFromRowAsTreeNode(Row row, String stringCells, List<String> stack, int increment) {
        String stringRow;
        Cell cell = row.getCell(row.getFirstCellNum());
        if (cell.getCellType() == Cell.CELL_TYPE_STRING && cell.getStringCellValue().length() > 0) {
            stringRow = jsonApplyIndentationAsString(stringCells, row, stack, increment);
        } else {
            stringRow = "{" + SPREAD_SHEET_ROW + " : " + stringCells + "}";
        }   
        return stringRow;
    }
    
    /**
     * 
     * @param row
     * @param cells
     * @throws JSONException 
     */
    private JSONArray jsonFromRow(Row row) throws JSONException {
        JSONArray cells = new JSONArray();
        
        for (Iterator<Cell> cellsIT = row.cellIterator(); cellsIT.hasNext();) {
            Cell cell = cellsIT.next();
            
            if (cell.getCellType() == Cell.CELL_TYPE_STRING
                    || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
                cells.put(cell.getStringCellValue());
            } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                cells.put(cell.getNumericCellValue());
            } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
                cells.put(cell.getBooleanCellValue());
            } else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {
                cells.put(cell.getErrorCellValue());
            } else {
                cells.put("N/A");
            }
        }
        
        return cells;
    }    
    
    private String jsonFromRowAsString(Row row) throws JSONException {
        StringBuilder cells = new StringBuilder();
        Boolean first = Boolean.TRUE;
        Boolean dirty = Boolean.FALSE;
        
//        cells.append("[");
        int i = 0;
        for (Iterator<Cell> cellsIT = row.cellIterator(); cellsIT.hasNext();) {
           
            if(dataFieldNames().size() <= i)
                break;
            
            Cell cell = cellsIT.next();
            
            if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append(quotes(cell.getStringCellValue()));
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append(quotes(cell.getStringCellValue()));
                }
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append(quotes(cell.getStringCellValue()));
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append(quotes(cell.getStringCellValue()));
                }               
            } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {                
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append(cell.getNumericCellValue());
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append(cell.getNumericCellValue());
                }
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {               
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append(cell.getBooleanCellValue());
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append(cell.getBooleanCellValue());
                }
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {               
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append(cell.getErrorCellValue());
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append(cell.getErrorCellValue());
                }
                dirty = Boolean.TRUE;
            } else {               
                if (first) {
                    cells.append(key(dataFieldNames().get(i))).append("N/A");
                    first = Boolean.FALSE;
                } else {
                    cells.append(",").append(key(dataFieldNames().get(i))).append("N/A");
                }
                dirty = Boolean.TRUE;
            }
            i++;
        }
        
        if(dirty){
            return "{" + cells.append("}").toString();            
        } else {        
            return "";
        }
    }    
   
    /**
     * 
     * @param jsonCells
     * @param row
     * @param stack
     * @param increment
     * @return
     * @throws JSONException 
     */
    private JSONObject applyIndentation(JSONArray jsonCells, Row row, List<String> stack, int increment) throws JSONException {
        JSONObject jsonRow = new JSONObject();
        
        Cell cell       = row.getCell(row.getFirstCellNum());
        String nodeName = cell.getStringCellValue() + "";
        int indent      = cell.getCellStyle().getIndention();
        int absIndent   = indent/increment;
        
        if(indent == 0){
            jsonRow = jsonRow.put(nodeName + "", jsonCells);
        } else if(absIndent > (stack.size() - 1)){
            jsonRow = buildNestedJsonObject(jsonRow, nodeName, jsonCells, stack);
        } else {
            // remove items from the top of the stack
            for(int i = stack.size(); i > absIndent; i--){
                stack.remove(i - 1);
            }
            jsonRow = buildNestedJsonObject(jsonRow, nodeName, jsonCells, stack);
        }
        
        return jsonRow;
    }
    
    /**
     * 
     * @param jsonCells
     * @param row
     * @param stack
     * @param increment
     * @return 
     */
    private String jsonApplyIndentationAsString(String stringCells, Row row, List<String> stack, int increment){
        StringBuilder stringBuilder = new StringBuilder();
        
        Cell cell       = row.getCell(row.getFirstCellNum());
        String nodeName = cell.getStringCellValue();
        
        if(nodeName.isEmpty()){
            nodeName = DEFAULT_NODE_NAME;
        }
        
        int indent      = cell.getCellStyle().getIndention();
        int absIndent   = indent/increment;
        
        if(absIndent > 0 && absIndent <= (stack.size() - 1)){
             // remove items from the top of the stack
            for(int i = stack.size() - 1; i >= absIndent; --i){
                stack.remove(i);
            }
            stack.add(nodeName);
            stringBuilder = buildNestedJsonString(stringCells, stack);
        } else if(absIndent > (stack.size() - 1)){
            stack.add(nodeName);
            stringBuilder = buildNestedJsonString(stringCells, stack);
        } else if(absIndent == 0){
            stringBuilder = stringBuilder.append("{" + "\"").append(nodeName.replace('.', '_')).append("\"" + " : ").append(stringCells).append("}");
            stack.removeAll(stack);
            stack.add(nodeName);
        }
        
        return stringBuilder.toString();
    }
    
    /**
     * 
     * @param jsonRow
     * @param nodeName
     * @param jsonCells
     * @param stack
     * @return
     * @throws JSONException 
     */
    private JSONObject buildNestedJsonObject(JSONObject jsonRow, String nodeName, JSONArray jsonCells, List<String> stack) throws JSONException {
        jsonRow = jsonRow.put(nodeName + "", jsonCells.toString());
        for(int i = stack.size(); i > 0; i--){
            jsonRow = jsonRow.put(stack.get(i - 1), jsonRow.toString()) ;
        }
        stack.add(nodeName);
        return jsonRow;
    }

    /**
     * 
     * @param stringBuilder
     * @param nodeName
     * @param stringCells
     * @param stack
     * @return 
     */
    private StringBuilder buildNestedJsonString(String stringCells, List<String> stack) {
        StringBuilder stringBuilder = new StringBuilder();        
        
        for(int i = 0; i < stack.size(); i++){
            if(dataFieldNames().size() > i){
                stringBuilder = buildTreeBrunch(stringBuilder, stack.get(i), i);
            } else {
                stringBuilder = stringBuilder.append("{").append(quotes(stack.get(i))).append(" : ");
            }
        }
        
        stringBuilder.append(stringCells);
        
        // Close all "{" with "}"
        for(int i = 0; i < stack.size(); i++){
            stringBuilder = stringBuilder.append("}");
        }
               
        return stringBuilder;
    }

    
    private StringBuilder buildTreeBrunch(StringBuilder stringBuilder, String item, int i) {
        
        if (i == 1) {
            String[] names = treeNodeNames().get(i).split(" ");
            String[] values = item.split(" ");
            
            if (names.length > 1 && values.length > 7) {
                stringBuilder.append("{")
                        .append(key(names[0]))
                        .append(quotes(values[6]))
                        .append(", ")
                        .append(key(names[1]))
                        .append(quotes(values[4]))
                        .append(", ")
                        .append(key(values[4]));
            } else {
                stringBuilder = stringBuilder.append("{").append(quotes(item)).append(" : ");
            }
        } else {
            stringBuilder.append("{")
                    .append(key(treeNodeNames().get(i)))
                    .append(quotes(item))
                    .append(", ")
                    .append(key(item));
        }
        
        return stringBuilder;
    }

    private String quotes(String string) {
         return "\"" + string.trim() + "\"";
    }
    
    private String key(String string) {
         return "\"" + string.trim() + "\": ";
    }
 
    
    /**
     * Returns mongo collection using supplied properties. 
     * @param properties
     * @return
     * @throws UnknownHostException 
     */
//    public DBCollection mongoCollection(Properties properties) throws UnknownHostException {
//        DBCollection collection = null;
//
//        Mongo mongoDb;
//        
//        String host = properties.getProperty(HOST);
//        Integer port = getInteger(properties, PORT);
//        String dbName = properties.getProperty(MONGO_DB_NAME);
//        String collectionName = properties.getProperty(MONGO_COLLECTION);
//
//        mongoDb = new Mongo(host, port);
//        DB db = mongoDb.getDB(dbName);
//        collection = db.getCollection(collectionName);
//
//        return collection;
//    }
  
    /** 
     * Retrieves an integer value. 
     * @param key The key name. 
     * @return The requested value (<code>null</code> if not found). 
     */
    public Integer getInteger(Properties props, String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }
        Integer value = null;
        String string = props.getProperty(key);
        if (string != null) {
            value = new Integer(string);
        }
        return value;
    }

    /**
     * @return the dataFieldNames
     */
    public abstract List<String> dataFieldNames();

    /**
     * @param aDataFieldNames the dataFieldNames to set
     */
    public abstract List<String> treeNodeNames();

}
