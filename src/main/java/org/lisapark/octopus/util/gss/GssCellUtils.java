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
package org.lisapark.octopus.util.gss;

import com.google.common.collect.Maps;
import com.google.gdata.client.spreadsheet.CellQuery;
import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.lisapark.octopus.core.event.Attribute;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class GssCellUtils {
    
    private final String DEFAULT_DELIMITER = ":";

    private String userEmail;
    private String password;
    
    /** Our view of Google Spreadsheets as an authenticated Google user. */
    private SpreadsheetService service;
    /** The URL of the cells feed. */
    private URL cellFeedUrl;
    /** A factory that generates the appropriate feed URLs. */
    private FeedURLFactory factory;
    
    private String workSheetName;
    private String spreadSheetName;
    private String rangeName;
    
    private String topLeftCell;
    private String bottomRightCell;
    
    private Boolean loggedIn = Boolean.FALSE;
   
     
    public static void main(String[] args) {
        
        GssCellUtils gssCell = new GssCellUtils("test",
                "USFutures15minuteIntervalQuery",
                "Sheet1",
                "alexmy@lisa-park.com",
                "murzik1991",
                "2:4",
                "2008:7");
        try {
            gssCell.loadSheet();
            List<Map<String, Object>> list = gssCell.getCellRangeAsListStr(gssCell.getTestAttrList());
            
            System.out.println(list);
            
        } catch (ServiceException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }        
    }
    
    /**
     * 
     * @param serviceId
     * @param userEmail
     * @param password
     * @param rangeName 
     */
    public GssCellUtils(
            String serviceId,
            String workBook,
            String spreadSheet,
            String userEmail, 
            String password, 
            String rangeName) {
        this.userEmail = userEmail;
        this.password = password;
        this.rangeName = rangeName;

        this.service = new SpreadsheetService(serviceId);
        try {
            // Authenticate
            service.setUserCredentials(userEmail, password);
            this.loggedIn = Boolean.TRUE;
        } catch (AuthenticationException ex) {
            Exceptions.printStackTrace(ex);
        }
        
        this.factory = FeedURLFactory.getDefault();
    }

    /**
     * 
     * @param serviceId
     * @param userEmail
     * @param password
     * @param topLeftCell
     * @param bottomRightCell 
     */
    public GssCellUtils(
            String serviceId,            
            String spreadSheet,
            String workSheet,
            String userEmail, 
            String password, 
            String topLeftCell, 
            String bottomRightCell) {
        
        this.spreadSheetName = spreadSheet;
        this.workSheetName = workSheet;
        this.userEmail = userEmail;
        this.password = password;
        this.topLeftCell = topLeftCell;
        this.bottomRightCell = bottomRightCell;

        this.service = new SpreadsheetService(serviceId);
        try {
            // Authenticate
            service.setUserCredentials(userEmail, password);
            this.loggedIn = Boolean.TRUE;
        } catch (AuthenticationException ex) {
            Exceptions.printStackTrace(ex);
        }
        
        this.factory = FeedURLFactory.getDefault();
    }
       
    /**
     * @return the service
     */
    public SpreadsheetService getService() {
        return service;
    }

    /**
     * @return the cellFeedUrl
     */
    public URL getCellFeedUrl() {
        return cellFeedUrl;
    }

    /**
     * @return the factory
     */
    public FeedURLFactory getFactory() {
        return factory;
    }

    /**
     * @return the workBookName
     */
    public String getWorkBookName() {
        return workSheetName;
    }

    /**
     * @param workBookName the workBookName to set
     */
    public void setWorkBookName(String workBookName) {
        this.workSheetName = workBookName;
    }

    /**
     * @return the workSheetName
     */
    public String getWorkSheetName() {
        return spreadSheetName;
    }

    /**
     * @param workSheetName the workSheetName to set
     */
    public void setWorkSheetName(String workSheetName) {
        this.spreadSheetName = workSheetName;
    }

    /**
     * @return the rangeName
     */
    public String getRangeName() {
        return rangeName;
    }

    /**
     * @param rangeName the rangeName to set
     */
    public void setRangeName(String rangeName) {
        this.rangeName = rangeName;
    }

    public void loadSheet(String spreadSheetName, String workSheetName) throws ServiceException, IOException {
        // Get the spreadsheet to load
        SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(),
                SpreadsheetFeed.class);
        List<SpreadsheetEntry> spreadsheets = feed.getEntries();
        int spreadsheetIndex = getSpreadsheetIndex(spreadsheets, spreadSheetName);
        SpreadsheetEntry spreadsheet = feed.getEntries().get(spreadsheetIndex);

        // Get the worksheet to load
        if (spreadsheet.getWorksheets().size() == 1) {
            cellFeedUrl = spreadsheet.getWorksheets().get(0).getCellFeedUrl();
        } else {
            List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
            int worksheetIndex = getWorksheetIndex(worksheets, workSheetName);
            WorksheetEntry worksheet = (WorksheetEntry) worksheets.get(worksheetIndex);
            cellFeedUrl = worksheet.getCellFeedUrl();
        }
        
    }
    
    public void loadSheet() throws ServiceException, IOException {
        // Get the spreadsheet to load
        SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(),
                SpreadsheetFeed.class);
        List<SpreadsheetEntry> spreadsheets = feed.getEntries();
        int spreadsheetIndex = getSpreadsheetIndex(spreadsheets, spreadSheetName);
        SpreadsheetEntry spreadsheet = feed.getEntries().get(spreadsheetIndex);

        // Get the worksheet to load
        if (spreadsheet.getWorksheets().size() == 1) {
            cellFeedUrl = spreadsheet.getWorksheets().get(0).getCellFeedUrl();
        } else {
            List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
            int worksheetIndex = getWorksheetIndex(worksheets, workSheetName);
            WorksheetEntry worksheet = (WorksheetEntry) worksheets.get(worksheetIndex);
            cellFeedUrl = worksheet.getCellFeedUrl();
        }
        
    }
    
    /**
     * 
     * @param attributes
     * @return
     * @throws ServiceException 
     */
    public List<Map<String, Object>> getCellRangeAsList(List<Attribute> attributes)
            throws ServiceException {

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        CellQuery query = new CellQuery(cellFeedUrl);
        
        query.setMinimumRow(getRowIndex(topLeftCell));
        query.setMaximumRow(getRowIndex(bottomRightCell));
        query.setMinimumCol(getColumnIndex(topLeftCell));
        query.setMaximumCol(getColumnIndex(bottomRightCell));
        
        try {
            CellFeed feed = service.query(query, CellFeed.class);

            int rowRange = query.getMaximumRow() - query.getMinimumRow();
            int columnRange = query.getMaximumCol() - query.getMinimumCol();
            
            Map<String, Object> map = Maps.newHashMap();

            List<CellEntry> listEntry = feed.getEntries();
            
            int ind = 0;
            while(ind < rowRange * columnRange){    
                for (Attribute attribute : attributes) {
                    map.put(attribute.getName(), listEntry.get(ind).getCell().getValue());
                    ind++;
                }
                
                list.add(Maps.newHashMap(map));
            }
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }
        return list;
    }

    private int getSpreadsheetIndex(List<SpreadsheetEntry> spreadsheets, String itemName) {
        int index = 0;
        
        String itemNameTrimmed = itemName.trim();
        for(SpreadsheetEntry item : spreadsheets){
            if(item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) break;
            index++;
        }
        return index;
    }

    private int getWorksheetIndex(List<WorksheetEntry> worksheets, String workSheetName) {
        int index = 0;
        
        String itemNameTrimmed = workSheetName.trim();
        for(WorksheetEntry item : worksheets){
            if(item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) break;
            index++;
        }
        return index;
    }

    private Integer getRowIndex(String topLeftCell, String delimeter) {
        String[] pair = topLeftCell.split(delimeter);        
        return Integer.parseInt(pair[0]);
    }
    
    private Integer getRowIndex(String topLeftCell) {
        String[] pair = topLeftCell.split(DEFAULT_DELIMITER);        
        return Integer.parseInt(pair[0]);
    }
    
    private Integer getColumnIndex(String topLeftCell, String delimeter) {
        String[] pair = topLeftCell.split(delimeter);        
        return Integer.parseInt(pair[1]);
    }    
     
    private Integer getColumnIndex(String topLeftCell) {
        String[] pair = topLeftCell.split(DEFAULT_DELIMITER);        
        return Integer.parseInt(pair[1]);
    }

    private List<String> getTestAttrList() {
        List<String> list = new ArrayList<String>();
        
        list.add("open");
        list.add("high");
        list.add("low");
        list.add("close");
        
        return list;
    }

    private List<Map<String, Object>> getCellRangeAsListStr(List<String> testAttrList) 
            throws ServiceException {

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        CellQuery query = new CellQuery(cellFeedUrl);
        
        query.setMinimumRow(getRowIndex(topLeftCell));
        query.setMaximumRow(getRowIndex(bottomRightCell));
        query.setMinimumCol(getColumnIndex(topLeftCell));
        query.setMaximumCol(getColumnIndex(bottomRightCell));
        
        try {
            CellFeed feed = service.query(query, CellFeed.class);

            int rowRange = query.getMaximumRow() - query.getMinimumRow();
            
            Map<String, Object> map = Maps.newHashMap();
//            for (CellEntry entry : feed.getEntries()) {
            List<CellEntry> listEntry = feed.getEntries();
            
            int ind = 0;
            while(ind < rowRange){    
                for (String attribute : testAttrList) {
                    map.put(attribute, listEntry.get(ind).getCell().getValue());
                    ind++;
                }                
                list.add(Maps.newHashMap(map));
            }
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }
        return list;
    }
   
}
