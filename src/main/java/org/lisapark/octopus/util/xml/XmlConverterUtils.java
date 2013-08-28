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
package org.lisapark.octopus.util.xml;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.sleepycat.dbxml.XmlContainer;
//import com.sleepycat.dbxml.XmlManager;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.json.JSONException;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.util.json.ExcelUtils;
import org.openide.util.Exceptions;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class XmlConverterUtils {
    private static final String IPLAST_LEFT_TAG     = "<iplast>";
    private static final String IPLAST_RIGHT_TAG    = "</iplast>";
    
    private static final String SHIFT               = "shift";
    
    private static final String DATE_TAG_NAME       = "date";    
    private static final String SHIFTID_TAG_NAME    = "shiftId";
    
    private static final int PRODUCTION_OUTLINE_INCREAMENT  = 2;
    private static final int WAREHOUSE_OUTLINE_INCREAMENT   = 1;
    private static final int WAREHOUSE_DATE_ROW_SHIFT       = 3;
    
    static List<String> dataFieldNames;
    
    static List<String> treeNodeNames;    
         
    public static void main(String[] args) throws ProcessingException, SQLException {
        
//        String fileName = "C:/Users/Alex/Documents/NetBeansProjects/FROM_NJ_SVN/octopus-1.0-dist/octopus-designer/bin/TestExcel_001.xls";
       
        String fileName = "C:/Users/Alex/Documents/NetBeansProjects/FROM_NJ_SVN/"
                + "octopus-1.0-dist/octopus-designer/bin/warehouse_001.xls";
        
        String docName = "Warehouse_001";
        
        // An empty string means an in-memory container, which
	// will not be persisted
	String containerName = "octopus";
	
//	XmlManager mgr = null;
//	XmlContainer cont = null;
        
        try {
//            HSSFWorkbook book = ExcelUtils.excelWorkbookFromFile(fileName);
            Properties props = new Properties();
            
            props.put(ExcelUtils.CLASS_NAME,    "com.mysql.jdbc.Driver");
            props.put(ExcelUtils.USER_NAME,     "root");
            props.put(ExcelUtils.PASSWORD,      "lisa1234");
            props.put(ExcelUtils.URL,           "jdbc:mysql://173.72.110.131:3306/webdav");
            props.put(ExcelUtils.SELECT_SQL,    "SELECT CONTENT, ID FROM REPOSITORY WHERE NAME LIKE ?");
            props.put(ExcelUtils.UPDATE_SQL,    "UPDATE REPOSITORY SET SCANNED = 1 WHERE ID = ?");
            props.put(ExcelUtils.FILE_NAME,     "TestExcel");
                        
            List<HSSFWorkbook> books;
            books = ExcelUtils.allUnscannedExcelWorkbooks(props);
            String xml = xmlStringFromProductionSS(books.get(0), 0, 0);
//            String xml = xmlStringFromWarehouseSS(books.get(0), 0, 0);
//            String xml = xmlStringFromWarehouseSS(book, 0, 0);
            
            System.out.println(xml);

            // All BDB XML programs require an XmlManager instance
//	    mgr = new XmlManager();
             
            List<Map<String, Object>> mapList = eventsFromProductionXml(xml);

            System.out.println("Map List: " + mapList);
            
            //==================================================================
//            if (mgr.existsContainer(containerName) != 0) {
//                cont = mgr.openContainer(containerName);               
//            } else {
//                cont = mgr.createContainer(containerName);
//            }            
//            // Check if document is in db
//            try{
//                 // Now, get the document
//                XmlDocument doc0 = cont.getDocument(docName);
//                String name0 = doc0.getName();
//                String docContent0 = doc0.getContentAsString();
//                // print it
//                System.out.println("Document name0: " + name0 
//                        + "\nContent0: " + docContent0);
//            } catch(XmlException xe){
//                if(xe.getErrorCode() == XmlException.DOCUMENT_NOT_FOUND){
//                    cont.putDocument(docName, xml);
//                }
//            }
//            
//	    // Now, get the document
//	    XmlDocument doc = cont.getDocument(docName);
//	    String name = doc.getName();
//	    String docContent = doc.getContentAsString();
//	    // print it
//	    System.out.println("Document name: " + name 
//                    + "\nContent: " + docContent);
	} catch (SAXException ex) {
            Exceptions.printStackTrace(ex);
        } catch (ParserConfigurationException ex) {
            Exceptions.printStackTrace(ex);
//        } catch (DOMException ex) {
//            Exceptions.printStackTrace(ex);
        } catch (JSONException ex) {
            Exceptions.printStackTrace(ex);
        } catch (FileNotFoundException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
//        } catch (XmlException xe) {            
//	    System.err.println("XmlException: " + xe.getMessage());
	} finally {
//	    cleanup(mgr, cont);
	}
    }
    
    // This function is used to ensure that databases are
    // properly closed, even on exceptions
//    private static void cleanup(XmlManager mgr, XmlContainer cont) {
//	try {
//	    if (cont != null) {
//                cont.close();
//            }
//	    if (mgr != null) {
//                mgr.close();
//            }
//	} catch (Exception e) {
//	    // ignore exceptions in cleanup
//	}
//    }
    
    /**
     * 
     * @param book
     * @param index
     * @return
     * @throws JSONException
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static String xmlStringFromProductionSS(HSSFWorkbook book, int index, int increament) 
            throws JSONException, FileNotFoundException, IOException {
        // Get Sheet by index.
        if (book.getNumberOfSheets() > index) {
            Sheet sheet = book.getSheetAt(index);
            if(increament > 0){
                return xmlNodesProduction(sheet, increament);
            } else {
                return xmlNodesProduction(sheet, PRODUCTION_OUTLINE_INCREAMENT);
            }
        } else {
            return null;
        }
    }
    
    public static String xmlStringFromWarehouseSS(HSSFWorkbook book, int index, int increament) 
            throws JSONException, FileNotFoundException, IOException {
        // Get Sheet by index.
        if (book.getNumberOfSheets() > index) {
            Sheet sheet = book.getSheetAt(index);
            if(increament > 0){
                return xmlNodesWarehouse(sheet, increament);
            } else {
                return xmlNodesWarehouse(sheet, WAREHOUSE_OUTLINE_INCREAMENT);
            }
        } else {
            return null;
        }
    }

    public static String xmlTagAttributesFromSS(Sheet sheet, int increment) throws JSONException {
        // Return null, if sheet is null
        if(sheet == null) {
            return null;
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
            if (row.getPhysicalNumberOfCells() <= 0 || row.getRowNum() < splitRowNumber) {
                continue;
            }
            String tagAttributes = tagAttributesAsString(row);
            if (tagAttributes.isEmpty()) {
                continue;
            }
            String stringRow = xmlFromRowAsTreeAttributes(tagAttributes, row, stack, increment);
            if (first) {
                stringBuilderRows.append(IPLAST_LEFT_TAG).append(stringRow);
                first = Boolean.FALSE;
            } else {
                stringBuilderRows.append(stringRow);
            }            
            System.out.println(stringRow);            
        } 
        
        // Close all opened tags from stack
        if(!stack.isEmpty()){
            int n = stack.size();
            for(int i = n - 1; i >= 0; --i){
                stringBuilderRows = stringBuilderRows.append(rightTag(stack.get(i)));
            }
        }        
        // Get the XML text.
        stringBuilderRows = stringBuilderRows.append(IPLAST_RIGHT_TAG);
        
        return stringBuilderRows.toString();
    }

    
    private static String xmlNodesProduction(Sheet sheet, int increment) throws JSONException {
        dataFieldNames = Lists.<String>newArrayList("name", "field2", "fiels3", "field4",
                "category", "TOTAL_MATERIALS", "TOTAL_PRODUCTS");        
        treeNodeNames = Lists.<String>newArrayList("SHOP", "SHIFT", "MACHINE",
                "PRODUCT", "PRODUCT_TYPE", "RAW_MATERIAL"); 
        
        return IPLAST_LEFT_TAG + xmlTagNodesFromSSheet(sheet, increment, -1, 0) + IPLAST_RIGHT_TAG;
    }
    
    private static String xmlNodesWarehouse(Sheet sheet, int increment) throws JSONException {
        dataFieldNames  = Lists.<String>newArrayList("name", "BEGINING", "INCOMING",
                "OUTGOING", "ENDING");
        treeNodeNames   = Lists.<String>newArrayList("WAREHOUSE", "ITEM", "ITEM_TYPE", "NAME");
        
        StringBuilder stringBuilder = new StringBuilder();
        int start       = 0;
        int rangeLen    = 4;
        int rangeStart  = 0;
        
        List<String> dateList = getDateList(sheet, start, rangeLen);
        for(String date : dateList ){
            stringBuilder.append(leftTagWithAttributes("date", "value=" + "\"" + date + "\"")) 
                    .append(xmlTagNodesFromSSheet(sheet, increment, rangeStart, rangeLen))
                    .append(rightTag("date"));
            
            rangeStart = rangeStart + rangeLen;
//            break;
        }
        return IPLAST_LEFT_TAG + stringBuilder.toString() + IPLAST_RIGHT_TAG;
    }
    
    
    private static List<String> getDateList(Sheet sheet, int start, int rangeLen) {
        List<String> dateList = Lists.newArrayList();        
          
        if (sheet.getPaneInformation() != null && sheet.getPaneInformation().isFreezePane()) {
            int splitRowNumber  = sheet.getPaneInformation().getHorizontalSplitPosition();            
            Row row             = sheet.getRow(splitRowNumber - WAREHOUSE_DATE_ROW_SHIFT);
            int currCellNumber  = row.getFirstCellNum() + start + 1;
            
            while(currCellNumber <= row.getPhysicalNumberOfCells()){
                dateList.add(row.getCell(currCellNumber).getStringCellValue());
                currCellNumber+= rangeLen;
            }
        } else {
            dateList = null;
        }        
        return dateList;
    }
    
    /**
     * 
     * @param row
     * @return
     * @throws JSONException 
     */
    private static String tagAttributesAsString(Row row) throws JSONException {
        StringBuilder cells = new StringBuilder();
        Boolean dirty = Boolean.FALSE;
        
        int i = 0;
        for (Iterator<Cell> cellsIT = row.cellIterator(); cellsIT.hasNext();) {
            if (dataFieldNames.size() <= i) {
                break;
            }
            Cell cell = cellsIT.next();
            if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                cells.append(attribute(dataFieldNames.get(i), cell.getStringCellValue()));
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
//                cells.append(attribute(dataFieldNames.get(i), cell.getStringCellValue()));
            } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                cells.append(attribute(dataFieldNames.get(i), cell.getNumericCellValue()));
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
                cells.append(attribute(dataFieldNames.get(i), cell.getBooleanCellValue()));
                dirty = Boolean.TRUE;
            } else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {
                cells.append(attribute(dataFieldNames.get(i), cell.getErrorCellValue()));
                dirty = Boolean.TRUE;
            } else {
                cells.append(attribute(dataFieldNames.get(i), "N/A"));
                dirty = Boolean.TRUE;
            }
            i++;
        }        
        if(dirty){
            return cells.toString();            
        } else {        
            return "";
        }
    }
    
    private static String tagNodesAsString(Row row, int dataRangeStart, int dataRangeLen) throws JSONException {
        StringBuilder nodeStringBuilder = new StringBuilder();
        StringBuilder rowStringBuilder = new StringBuilder();
        
        int start;
        int end;
        
        // Define start and end points of cell range to be converted
        if(dataRangeStart == -1){
            start = row.getFirstCellNum();
        } else {
            start = row.getFirstCellNum() + dataRangeStart;
        }
        if(dataRangeLen == 0){
            end = row.getPhysicalNumberOfCells();
        } else {
            end = start + dataRangeLen;
        }        
        int i = 0;
        int j = 0;
        // Itarate over cell range and build xml nodes
        for (Iterator<Cell> cellsIT = row.cellIterator(); cellsIT.hasNext();) {
            if (i >= end) {
                break;
            }            
            Cell cell = cellsIT.next();
            // Skip cells that are out of range
            if(i > 0 && i < start){
                i++;
                continue;
            } 
            // Build all nodes from Spreadsheet row with specified cell range
            rowStringBuilder = rowStringBuilder.append(buidNodeAsString(cell, i, j));            
            i++;
            j++;
        }        
        return nodeStringBuilder.append(rowStringBuilder.toString()).toString();        
    }
    
    private static String extractDateAndShiftAsNodes(String fieldValue) {

        StringBuilder stringBuilder = new StringBuilder();
        String[] values = fieldValue.split(" ");

        if (values != null && values.length > 7) {
            stringBuilder.append(node(DATE_TAG_NAME, values[6])).append(node(SHIFTID_TAG_NAME, values[4]));
            return stringBuilder.toString();
        } else {
            return null;
        }
    }
    
    /**
     * 
     * @param attrName
     * @param attrValue
     * @return 
     */
    private static String attribute(String attrName, Object attrValue){
        return " " + attrName + "=" + "\"" + attrValue + "\"";
    }
    
    /**
     * 
     * @param attrName
     * @param attrValue
     * @return 
     */
    private static String node(String attrName, Object attrValue){
        return "<" + attrName + ">" + "\"" + attrValue + "\"" + "</" + attrName + ">";
    }
     
    /**
     * 
     * @param jsonCells
     * @param row
     * @param stack
     * @param increment
     * @return 
     */
    private static String xmlFromRowAsTreeAttributes(String tagAttributes, Row row, List<String> stack, int increment){
        String tagString = ""; 
        
        Cell cell       = row.getCell(row.getFirstCellNum());        
                
        int indent      = cell.getCellStyle().getIndention();
        int absIndent   = indent/increment;
        
        int diff = stack.size() - absIndent;
        
        if(diff == 0){
            tagString = leftTagWithAttributes(treeNodeNames.get(absIndent), tagAttributes);
            stack.add(treeNodeNames.get(absIndent));
        } else if(diff > 0){
            while(diff > 0){
                tagString = tagString + rightTag(stack.get(stack.size() - 1));
                stack.remove(stack.size() - 1);
                diff--;
            }            
            tagString = tagString + leftTagWithAttributes(treeNodeNames.get(absIndent), tagAttributes);
            stack.add(treeNodeNames.get(absIndent));
        }                
        return tagString;
    }
    
    /**
     * 
     * @param tagNodes
     * @param row
     * @param stack
     * @param increment
     * @return 
     */
    private static String xmlFromRowAsTreeNodes(String tagNodes, Row row, List<String> stack, int increment){
        String tagString = ""; 
        
        Cell cell       = row.getCell(row.getFirstCellNum());        
                
        int indent      = cell.getCellStyle().getIndention();
        int absIndent   = indent/increment;
        
        int diff = stack.size() - absIndent;
        
        if(diff == 0){
            tagString = leftTagWithNodes(treeNodeNames.get(absIndent), tagNodes);
            stack.add(treeNodeNames.get(absIndent));
        } else if(diff > 0){
            while(diff > 0){
                tagString = tagString + rightTag(stack.get(stack.size() - 1));
                stack.remove(stack.size() - 1);
                diff--;
            }            
            tagString = tagString + leftTagWithNodes(treeNodeNames.get(absIndent), tagNodes);
            stack.add(treeNodeNames.get(absIndent));
        }                
        return tagString;
    }
    
    /**
     * 
     * @param tagName
     * @return 
     */
    private static String rightTag(String tagName) {
        return "</" + tagName + ">";
    }

    /**
     * 
     * @param tagName
     * @param attributes
     * @return 
     */
    private static String leftTagWithAttributes(String tagName, String attributes) {
        return "<" + tagName + " " + attributes + ">";
    }
    
    /**
     * 
     * @param tagName
     * @param nodes
     * @return 
     */
    private static String leftTagWithNodes(String tagName, String nodes) {
        return "<" + tagName + ">" + nodes;
    }
    
    /**
     * 
     * @param tagName
     * @return 
     */
    private static String leftTag(String tagName) {
        return "<" + tagName + ">";
    }

    private static String xmlTagNodesFromSSheet(Sheet sheet, int increment, int dataRangeStart, int dataRangeLen) throws JSONException {
        // Return null, if sheet is null
        if(sheet == null) {
            return null;
        }
                     
        // Iterate through the rows.
        StringBuilder stringBuilderRows = new StringBuilder();        
        List<String> stack = new ArrayList<String>();
//        Boolean first = Boolean.TRUE;        
        int splitRowNumber = 0;        
        if (sheet.getPaneInformation() != null && sheet.getPaneInformation().isFreezePane()) {
            splitRowNumber = sheet.getPaneInformation().getHorizontalSplitPosition();
        }        
        for (Iterator<Row> rowsIT = sheet.rowIterator(); rowsIT.hasNext();) {
            Row row = rowsIT.next();
            if (row.getPhysicalNumberOfCells() <= 0 || row.getRowNum() < splitRowNumber) {
                continue;
            }
            String tagNodes = tagNodesAsString(row, dataRangeStart, dataRangeLen);
            if (tagNodes.isEmpty()) {
                continue;
            }
            String stringRow = xmlFromRowAsTreeNodes(tagNodes, row, stack, increment);
            stringBuilderRows.append(stringRow);  
        }         
        // Close all opened tags from stack
        if(!stack.isEmpty()){
            int n = stack.size();
            for(int i = n - 1; i >= 0; --i){
                stringBuilderRows = stringBuilderRows.append(rightTag(stack.get(i)));
            }
        } 
        
        return stringBuilderRows.toString();
    }

    private static String buidNodeAsString(Cell cell, int i, int j) {
        
        StringBuilder cellStringBuilder = new StringBuilder();
        
        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
            String string;
            if(i == 0 && cell.getCellStyle().getIndention() == 2 
                    && treeNodeNames.get(1).equalsIgnoreCase(SHIFT)){
                string = extractDateAndShiftAsNodes(cell.getStringCellValue());
                if(string != null){
                    cellStringBuilder.append(string);
                } else {
                    cellStringBuilder.append(node(dataFieldNames.get(j), cell.getStringCellValue()));
                }
            } else {
                cellStringBuilder.append(node(dataFieldNames.get(j), cell.getStringCellValue()));
            }           
        } else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
//                cells.append(attribute(dataFieldNames.get(i), cell.getStringCellValue()));
        } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
            cellStringBuilder.append(node(dataFieldNames.get(j), cell.getNumericCellValue()));            
        } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
            cellStringBuilder.append(node(dataFieldNames.get(j), cell.getBooleanCellValue()));            
        } else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {
            cellStringBuilder.append(node(dataFieldNames.get(j), cell.getErrorCellValue()));            
        } else {
            cellStringBuilder.append(node(dataFieldNames.get(j), "N/A"));            
        }
        
        return cellStringBuilder.toString();
    }

    
    public static List<Map<String, Object>> eventsFromWarehouseXml(String xml) throws SAXException, ParserConfigurationException, IOException, DOMException {
        
        DocumentBuilderFactory factory;
        factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(xml)));
        Element rootElement;
        rootElement = xmlDoc.getDocumentElement();
        List<Map<String, Object>> mapList = Lists.newArrayList();
        NodeList dateList = rootElement.getChildNodes();
        
        for (int j = 0; j < dateList.getLength(); j++) {
            Node date = dateList.item(j);
            String dateString = date.getAttributes().getNamedItem("value").getTextContent().replace("\"", "");

            NodeList warehouseList = date.getChildNodes();

            for (int i = 0; i < warehouseList.getLength(); i++) {
                Node warehouseNode = warehouseList.item(i);
                String warehouseString = getNodeName(warehouseNode);
                
                if(warehouseString.isEmpty()) {
                    continue;
                }

                NodeList itemList = warehouseNode.getChildNodes();

                HashMap<String, Object> warehouseMap = getNodeWarehouseDataMap(warehouseNode);
                warehouseMap.put("DATE", dateString);
                warehouseMap.put("WAREHOUSE", warehouseString);

                mapList.add(warehouseMap);

                for (int l = 0; l < itemList.getLength(); l++) {
                    
                    if(!"ITEM".equalsIgnoreCase(itemList.item(l).getNodeName())) {
                        continue;
                    }
                    
                    Node itemNode = itemList.item(l);
                    String itemString = getNodeName(itemNode);
                    
                    if(itemString.isEmpty()) {
                        continue;
                    }

                    NodeList itemTypeList = itemNode.getChildNodes();

                    HashMap<String, Object> itemMap = getNodeWarehouseDataMap(itemNode);
                    itemMap.put("DATE", dateString);
                    itemMap.put("WAREHOUSE", warehouseString);
                    itemMap.put("ITEM", itemString);

                    mapList.add(itemMap);
                    for (int k = 0; k < itemTypeList.getLength(); k++) {
                        
                        if(!"ITEM_TYPE".equalsIgnoreCase(itemTypeList.item(k).getNodeName())) {
                            continue;
                        }
                        
                        Node itemTypeNode = itemTypeList.item(k);
                        String itemTypeString = getNodeName(itemTypeNode);

                        if(itemTypeString.isEmpty()) {
                            continue;
                        }
                        
                        HashMap<String, Object> itemTypeMap = getNodeWarehouseDataMap(itemTypeNode);
                        itemTypeMap.put("DATE", dateString);
                        itemTypeMap.put("WAREHOUSE", warehouseString);
                        itemTypeMap.put("ITEM", itemString);
                        itemTypeMap.put("ITEM_TYPE", itemTypeString);

                        mapList.add(itemTypeMap);
                    }
                }
            }
        }
       
        return mapList;
    }
    
    private static HashMap<String, Object> getNodeWarehouseDataMap(Node node){
        HashMap<String, Object> map = Maps.newHashMap();
        NodeList nodeList = node.getChildNodes();
        for(int i = 0; i < nodeList.getLength(); i++){
            Node curNode = nodeList.item(i);
            if(qtyWarehouseConditions(curNode)){
                map.put(curNode.getNodeName(), curNode.getTextContent().replace("\"", ""));
            }
        }
        
        return map;
    }
        
    private static HashMap<String, Object> getNodeProdDataMap(Node node){
        HashMap<String, Object> map = Maps.newHashMap();
        NodeList nodeList = node.getChildNodes();
        for(int i = 0; i < nodeList.getLength(); i++){
            Node curNode = nodeList.item(i);
            if(qtyProductionConditions(curNode)){
                map.put(curNode.getNodeName(), curNode.getTextContent().replace("\"", ""));
            }
        }
        
        return map;
    }
    
    private static String getNodeName(Node node){
        String name = "";
        NodeList nodeList = node.getChildNodes();
        for(int i = 0; i < nodeList.getLength(); i++){
            Node curNode = nodeList.item(i);
            if("name".equalsIgnoreCase(curNode.getNodeName())){
                name = curNode.getTextContent().replace("\"", "");
                break;
            }
        }
        
        return name;
    }
    
    public static List<Map<String, Object>> eventsFromProductionXml(String xml) 
            throws SAXException, ParserConfigurationException, IOException, DOMException {
         
        DocumentBuilderFactory factory;
        factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(xml)));
        Element rootElement;
        rootElement = xmlDoc.getDocumentElement();
        List<Map<String, Object>> mapList = Lists.newArrayList();
        NodeList shopList = rootElement.getChildNodes();
        
        for (int j = 0; j < shopList.getLength(); j++) {
            Node shop = shopList.item(j);
            String shopString = getNodeName(shop);

            NodeList shiftList = shop.getChildNodes();

            for (int i = 0; i < shiftList.getLength(); i++) {
                
                if(!"SHIFT".equalsIgnoreCase(shiftList.item(i).getNodeName())) continue;
                
                Node shiftNode = shiftList.item(i);
                String shiftString = getShiftId(shiftNode);
                String dateString = getDate(shiftNode);
                
                if(shiftString.isEmpty()) continue;

                NodeList machineList = shiftNode.getChildNodes();

                HashMap<String, Object> shiftMap = getNodeProductionDataMap(shiftNode);
                shiftMap.put("DATE", dateString);
                shiftMap.put("SHOP", shopString);
                shiftMap.put("SHIFT", shiftString);

                mapList.add(shiftMap);

                for (int l = 0; l < machineList.getLength(); l++) {
                    
                    if(!"MACHINE".equalsIgnoreCase(machineList.item(l).getNodeName())) continue;
                    
                    Node machineNode = machineList.item(l);
                    String machineString = getNodeName(machineNode);
                    
                    if(machineString.isEmpty()) continue;

                    NodeList productsList = machineNode.getChildNodes();

                    HashMap<String, Object> machineMap = getNodeProductionDataMap(machineNode);
                    machineMap.put("DATE", dateString);
                    machineMap.put("SHOP", shopString);
                    machineMap.put("SHIFT", shiftString);
                    machineMap.put("MACHINE", machineString);

                    mapList.add(machineMap);
                    for (int k = 0; k < productsList.getLength(); k++) {
                        
                        if(!"PRODUCT".equalsIgnoreCase(productsList.item(k).getNodeName())) continue;
                        
                        Node productNode = productsList.item(k);
                        String productString = getNodeName(productNode);

                        if(productString.isEmpty()) continue;
                        
                        NodeList productTypeList = productNode.getChildNodes();
                        
                        HashMap<String, Object> productMap = getNodeProductionDataMap(productNode);
                        productMap.put("DATE", dateString);
                        productMap.put("SHOP", shopString);
                        productMap.put("SHIFT", shiftString);
                        productMap.put("MACHINE", machineString);
                        productMap.put("PRODUCT", productString);

                        mapList.add(productMap);
                        
                        for (int m = 0; m < productTypeList.getLength(); m++) {

                            if (!"PRODUCT_TYPE".equalsIgnoreCase(productTypeList.item(k).getNodeName())) {
                                continue;
                            }

                            Node productTypeNode = productTypeList.item(m);
                            String productTypeString = getNodeName(productTypeNode);

                            if (productTypeString.isEmpty()) {
                                continue;
                            }

                            NodeList rawMaterialsList = productTypeNode.getChildNodes();

                            HashMap<String, Object> productTypeMap = getNodeProductionDataMap(productTypeNode);
                            productTypeMap.put("DATE", dateString);
                            productTypeMap.put("SHOP", shopString);
                            productTypeMap.put("SHIFT", shiftString);
                            productTypeMap.put("MACHINE", machineString);
                            productTypeMap.put("PRODUCT", productString);
                            productTypeMap.put("PRODUCT_TYPE", productTypeString);

                            mapList.add(productTypeMap);
                            
                            for (int n = 0; n < productTypeList.getLength(); n++) {

                                if (!"RAW_MATERIAL".equalsIgnoreCase(productTypeList.item(k).getNodeName())) {
                                    continue;
                                }

                                Node rawMaterialNode = productTypeList.item(n);
                                String rawMaterialString = getNodeName(rawMaterialNode);

                                if (rawMaterialString.isEmpty()) {
                                    continue;
                                }

                                HashMap<String, Object> rawMaterialMap = getNodeProductionDataMap(rawMaterialNode);
                                rawMaterialMap.put("DATE", dateString);
                                rawMaterialMap.put("SHOP", shopString);
                                rawMaterialMap.put("SHIFT", shiftString);
                                rawMaterialMap.put("MACHINE", machineString);
                                rawMaterialMap.put("PRODUCT", productString);
                                rawMaterialMap.put("PRODUCT_TYPE", productTypeString);
                                rawMaterialMap.put("RAW_MATERIAL", rawMaterialString);

                                mapList.add(rawMaterialMap);
                            }
                        }
                    }
                }
            }
        }
       
        return mapList;
    }
    
    private static Boolean qtyWarehouseConditions(Node node) {
        return node.getNodeName().equalsIgnoreCase("BEGINING")
                || node.getNodeName().equalsIgnoreCase("INCOMING")
                || node.getNodeName().equalsIgnoreCase("OUTGOING")
                || node.getNodeName().equalsIgnoreCase("ENDING");
    }
    
    private static Boolean qtyProductionConditions(Node node) {
        return node.getNodeName().equalsIgnoreCase("RAW_MATERIAL")
                || node.getNodeName().equalsIgnoreCase("TOTAL_MATERIALS")
                || node.getNodeName().equalsIgnoreCase("TOTAL_PRODUCTS");
    }

    private static String getShiftId(Node node) {
        String name = "";
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node curNode = nodeList.item(i);
            if ("shiftId".equalsIgnoreCase(curNode.getNodeName())) {
                name = curNode.getTextContent().replace("\"", "");
                break;
            }
        }

        return name;
    }

    private static String getDate(Node node) {
        String date = "";
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node curNode = nodeList.item(i);
            if ("date".equalsIgnoreCase(curNode.getNodeName())) {
                date = curNode.getTextContent().replace("\"", "");
                break;
            }
        }

        return date;
    }

    private static HashMap<String, Object> getNodeProductionDataMap(Node node) {
        HashMap<String, Object> map = Maps.newHashMap();
        NodeList nodeList = node.getChildNodes();
        for(int i = 0; i < nodeList.getLength(); i++){
            Node curNode = nodeList.item(i);
            if(qtyProductionConditions(curNode)){
                map.put(curNode.getNodeName(), curNode.getTextContent().replace("\"", ""));
            }
        }
        
        return map;
    }

}
