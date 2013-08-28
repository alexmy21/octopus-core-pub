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

import com.google.common.collect.Maps;
import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ExcelSardineUtils {
    
    // Common field names
    public static final String RECORD_UUID      = "RECORD_UUID";
    public static final String CHRCKSUM         = "CHRCKSUM";
    public static final String CREATION_DATE    = "CREATION_DATE";
    public static final String DATE             = "DATE";
    
    // PRODUCTION Master table field names
    public static final String SHOP             = "SHOP";
    public static final String SHIFT            = "SHIFT";
    public static final String MACHINE          = "MACHINE";
    public static final String PRODUCT          = "PRODUCT";
    public static final String PRODUCT_TYPE     = "PRODUCT_TYPE";
    public static final String MATERIAL_TYPE    = "MATERIAL_TYPE";
    public static final String RAW_MATERIAL     = "RAW_MATERIAL";
    public static final String TOTAL_MATERIALS  = "TOTAL_MATERIALS";
    public static final String TOTAL_PRODUCTS   = "TOTAL_PRODUCTS";
    
    // WAREHOUSE Master table field names
    public static final String WAREHOUSE        = "WAREHOUSE";
    public static final String ITEM             = "ITEM";
    public static final String ITEM_TYPE        = "ITEM_TYPE";
    public static final String BEGINING         = "BEGINING";
    public static final String INCOMING         = "INCOMING";
    public static final String OUTGOING         = "OUTGOING";
    public static final String ENDING           = "ENDING";
    
    public static final String PROCESSED        = "PROCESSED";
    public static final String TRUE             = "TRUE";
    public static int PROD_OUTLINE_INCREAMENT   = 2;
    public static int WRH_OUTLINE_INCREAMENT    = 1;
    
    public static void main(String[] args){
        
        Map<String, Integer> prodCellIndexMap = Maps.newHashMap();
        prodCellIndexMap.put(SHOP, 0);
        prodCellIndexMap.put(SHIFT, 0);
        prodCellIndexMap.put(MACHINE, 0);
        prodCellIndexMap.put(PRODUCT, 0);
        prodCellIndexMap.put(PRODUCT_TYPE, 0);
        prodCellIndexMap.put(MATERIAL_TYPE, 0);
        prodCellIndexMap.put(RAW_MATERIAL, 4);
        prodCellIndexMap.put(TOTAL_MATERIALS, 5);
        prodCellIndexMap.put(TOTAL_PRODUCTS, 6);
        
        Map<String, Integer> wrhCellIndexMap = Maps.newHashMap();
        wrhCellIndexMap.put(WAREHOUSE, 1);
        wrhCellIndexMap.put(ITEM, 1);
        wrhCellIndexMap.put(ITEM_TYPE, 1);
        wrhCellIndexMap.put(BEGINING, 0);
        wrhCellIndexMap.put(INCOMING, 1);
        wrhCellIndexMap.put(OUTGOING, 2);
        wrhCellIndexMap.put(ENDING, 3);
        
        try {
            String excelFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Warehouse/";

            // Get all xml files
            Sardine sardine = SardineFactory.begin("", "");
            List<DavResource> resources = sardine.getResources(excelFile);
            
            for (DavResource res : resources) {
                String url = res.getPath();
                        //getAbsoluteUrl();
                if(res.isDirectory()) {
                    continue;
                } else {
                    Map<String, String> props = res.getCustomProps();
                    if (props.get(PROCESSED) == null) {                        
                        InputStream isData = sardine.get(url);
                        HSSFWorkbook book = new HSSFWorkbook(isData);
                        
                        int index = 0;
                        int increament = 1;
                        if (book.getNumberOfSheets() > index) {
                            if (increament == 0) {
//                                increament = PROD_OUTLINE_INCREAMENT;
                                increament = WRH_OUTLINE_INCREAMENT;
                            }
                            Sheet sheet = book.getSheetAt(index);
                            if (sheet == null) {
                                continue;
                            }

                            // Iterate through the rows.
                            int splitRowNumber = 0;
                            
                            if (sheet.getPaneInformation() != null 
                                    && sheet.getPaneInformation().isFreezePane()) {
                                splitRowNumber = sheet.getPaneInformation()
                                        .getHorizontalSplitPosition();
                            }

                            Map<String, Object> rowMap = Maps.newHashMap();
                            
                            int start = 2;
                            Row dateRow = sheet.getRow(8);
                            int end = dateRow.getLastCellNum();
                            
                            for (int dateShift = start; dateShift < end - 4; dateShift = dateShift + 4) {
                                
                                rowMap.put(DATE, formatDate(dateRow.getCell(dateShift).getStringCellValue()));
                                System.out.println(dateRow.getCell(dateShift).getStringCellValue());
                                
                                Sheet _sheet = book.getSheetAt(index);
                                
                                for (Iterator<Row> rowsIt = _sheet.rowIterator(); rowsIt.hasNext();) {
                                    Row row = rowsIt.next();
                                    if (row.getPhysicalNumberOfCells() <= 0 || row.getRowNum() < splitRowNumber) {
                                        continue;
                                    }

                                    Cell cell       = row.getCell(1);
                                    int indent      = cell.getCellStyle().getIndention();
                                    int absIndent   = indent / increament;
//                                
                                    if (processRowWrhSs(rowMap, row, wrhCellIndexMap, absIndent, dateShift)) {
                                        System.out.println(rowMap);
                                    }
                                }
                            }
                        }
                        props.put(PROCESSED, TRUE);
                        sardine.setCustomProps(url, props, null);
                    } else {
                        System.out.println("Property PROCESSED: " + props.get(PROCESSED));
                        List<String> removeProps = new ArrayList<String>(1);
                        removeProps.add(PROCESSED);
                        
                        sardine.setCustomProps(url, null, removeProps);
                    }
                    break;
                }
            }
        } catch (FileNotFoundException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        } 
    }
      
    /**
     * 
     * @param rowMap
     * @param row
     * @param cellIndexMap
     * @param increament
     * @return 
     */
    public static boolean processRowProdSs(Map<String, Object> rowMap, Row row, Map<String, Integer> cellIndexMap, int increament) {
        Boolean done = false;
        int cellIndex;
        
        switch (increament) {            
            case 0:
                // Clear all data from Map
                rowMap.clear();
                
                cellIndex = cellIndexMap.get(SHOP);
                rowMap.put(SHOP, (String) cellValue(row, cellIndex));
                break;
            case 1:
                // Remove all entries except SHOP, SHIFT and DATE
                rowMap.remove(MACHINE);
                rowMap.remove(PRODUCT);
                rowMap.remove(PRODUCT_TYPE);
                rowMap.remove(MATERIAL_TYPE);
                rowMap.remove(RAW_MATERIAL);
                rowMap.remove(TOTAL_MATERIALS);
                rowMap.remove(TOTAL_PRODUCTS);
                
                cellIndex = cellIndexMap.get(SHIFT);
                extractDateAndShift(rowMap, (String) cellValue(row, cellIndex));
                break;
            case 2:
                
                rowMap.remove(PRODUCT);
                rowMap.remove(PRODUCT_TYPE);
                rowMap.remove(MATERIAL_TYPE);
                rowMap.remove(RAW_MATERIAL);
                rowMap.remove(TOTAL_MATERIALS);
                rowMap.remove(TOTAL_PRODUCTS);
                
                cellIndex = cellIndexMap.get(MACHINE);
                rowMap.put(MACHINE, (String) cellValue(row, cellIndex));
                break;
            case 3:
                
                rowMap.remove(PRODUCT_TYPE);
                rowMap.remove(MATERIAL_TYPE);
                rowMap.remove(RAW_MATERIAL);
                rowMap.remove(TOTAL_MATERIALS);
                rowMap.remove(TOTAL_PRODUCTS);
                
                cellIndex = cellIndexMap.get(PRODUCT);
                rowMap.put(PRODUCT, (String) cellValue(row, cellIndex));
                break;
            // 
            case 4:
                
                rowMap.remove(MATERIAL_TYPE);
                rowMap.remove(RAW_MATERIAL);
                rowMap.remove(TOTAL_MATERIALS);
                
                cellIndex = cellIndexMap.get(PRODUCT_TYPE);
                rowMap.put(PRODUCT_TYPE, (String) cellValue(row, cellIndex));
                cellIndex = cellIndexMap.get(TOTAL_PRODUCTS);
                rowMap.put(TOTAL_PRODUCTS, cellValue(row, cellIndex));
                break;
            case 5:
                cellIndex = cellIndexMap.get(MATERIAL_TYPE);
                rowMap.put(MATERIAL_TYPE, (String) cellValue(row, cellIndex));
                cellIndex = cellIndexMap.get(RAW_MATERIAL);
                rowMap.put(RAW_MATERIAL, (String) cellValue(row, cellIndex));
                cellIndex = cellIndexMap.get(TOTAL_MATERIALS);
                rowMap.put(TOTAL_MATERIALS, cellValue(row, cellIndex));
                done = true;
                break;

        }

        return done;
    }
    
    
    public static boolean processRowWrhSs(Map<String, Object> rowMap, Row row, 
            Map<String, Integer> cellIndexMap, int increament, int dateIndex) {
        Boolean done = false;
        int cellIndex;
        
        switch (increament) {            
            case 0:
                
                cellIndex = cellIndexMap.get(WAREHOUSE);
                rowMap.put(WAREHOUSE, (String) cellValue(row, cellIndex));
                break;
            case 1:
                // Remove all entries except SHOP, SHIFT and DATE
                cellIndex = cellIndexMap.get(ITEM);
                rowMap.put(ITEM, (String) cellValue(row, cellIndex));
                break;
            case 2:
                
                rowMap.put(ITEM_TYPE, (String) cellValue(row, cellIndexMap.get(ITEM_TYPE)));
                rowMap.put(BEGINING, cellValue(row, cellIndexMap.get(BEGINING) + dateIndex));
                rowMap.put(INCOMING, cellValue(row, cellIndexMap.get(INCOMING) + dateIndex));
                rowMap.put(OUTGOING, cellValue(row, cellIndexMap.get(OUTGOING) + dateIndex));
                rowMap.put(ENDING, cellValue(row, cellIndexMap.get(ENDING) + dateIndex));
                
                done = true;
                
                break;         

        }

        return done;
    }
    /**
     * 
     * @param rowMap
     * @param fieldValue
     * @return 
     */
    public static Map<String, Object> extractDateAndShift(Map<String, Object> rowMap, String fieldValue) {

        String[] values = fieldValue.split(" ");

        if (values != null && values.length > 7) {
            String newDate = formatDate(values[6]);
            rowMap.put(DATE, newDate);
            rowMap.put(SHIFT, values[4]);
        }
        
        return rowMap;
    }

    /**
     * 
     * @param row
     * @param cellIndex
     * @return 
     */
    public static Object cellValue(Row row, int cellIndex) {
        Cell cell = row.getCell(cellIndex);

        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
            return cell.getStringCellValue();
        } else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
            return null;
        } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
            return cell.getNumericCellValue();
        } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
            return cell.getBooleanCellValue();
        } else if (cell.getCellType() == Cell.CELL_TYPE_ERROR) {
            return cell.getErrorCellValue();
        } else {
            return null;
        }
    }

    public static String formatDate(String values) {
        String[] dateParts = values.split("\\.");
        String newDate = dateParts[2] + "-" + dateParts[1] + "-" + dateParts[0];
        return newDate;
    }

}
