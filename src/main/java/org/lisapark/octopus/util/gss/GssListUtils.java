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
import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.ListQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.openide.util.Exceptions;



/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class GssListUtils {
    
    public static final String REC_NUMBER_NAME = "_recnum" ;
    
    private final String DEFAULT_DELIMITER = ":";

    private String userEmail;
    private String password;
    
    /** Our view of Google Spreadsheets as an authenticated Google user. */
    private SpreadsheetService service;
    /** The URL of the cells feed. */
    private URL listFeedUrl;
    /** A factory that generates the appropriate feed URLs. */
    private FeedURLFactory factory;
    
    private String workSheetName;
    private String spreadSheetName;
    
    private Map<String, ListEntry> entriesCached;
    
    public static void main(String[] args) {
        
        String newEntry = "RECNUM=3,A1=900,B1=0.02";
        String formulas = "MODEL1=A1 + B1*RECNUM";
        
        Map<String, String> entryMap = Maps.newHashMap();
        entryMap.put("A1", "800");
        entryMap.put("B1", "0.02");
                
        
        GssListUtils gssList = new GssListUtils("test",
                "demoForecast_USFutures15",
                "Forecast",
                "demo@lisa-park.com",
                "isasdemo");
        try {
            ListFeed listFeed = gssList.loadSheet();
          
            if(listFeed.getEntries().isEmpty()){
                ListEntry entry = gssList.addNewEntryValues(newEntry);                
                if (formulas != null && !formulas.isEmpty()) {
                    gssList.updateEntryFormulas(entry, newEntry, formulas);
                }
                gssList.getService().insert(gssList.getListFeedUrl(), entry);
            } else {
                ListEntry entry = gssList.updateEntryValues(listFeed.getEntries().get(0), newEntry);
                if (formulas != null && !formulas.isEmpty()) {
                    gssList.updateEntryFormulas(entry, newEntry, formulas);
                }
                entry.update();
//                gssList.getService().update(gssList.getListFeedUrl(), entry);
            }
            
            
                       
        } catch (ServiceException ex) {
            Exceptions.printStackTrace(ex);
        } catch (IOException ex) {
            Exceptions.printStackTrace(ex);
        }        
    }
       
    public static Object convert(String item) {
        Object obj = item;

        try {
            obj = Double.parseDouble(item);
        } catch (Exception e) {
            try{
                obj = Float.parseFloat(item);
            } catch(Exception ex) {
                try{
                    obj = Integer.parseInt(item);
                } catch(Exception exx){
                    // Do nothing let String go out
                }
            }
        }

        return obj;
    }

    
    public GssListUtils(String serviceId, String spreadSheet,
            String workSheet, String userEmail,  String password) {
        this.spreadSheetName    = spreadSheet;
        this.workSheetName      = workSheet;
        this.userEmail          = userEmail;
        this.password           = password;
        
        this.service = new SpreadsheetService(serviceId);
        try {
            service.setUserCredentials(userEmail, password);
        } catch (AuthenticationException ex) {
            Exceptions.printStackTrace(ex);
        }
        
        this.factory = FeedURLFactory.getDefault();
    }    
      
    public ListFeed loadSheet() throws ServiceException, IOException {
        // Get the spreadsheet to load
        SpreadsheetFeed feed = getService().getFeed(factory.getSpreadsheetsFeedUrl(),
                SpreadsheetFeed.class);
        List<SpreadsheetEntry> spreadsheets = feed.getEntries();
        int spreadsheetIndex = getSpreadsheetIndex(spreadsheets, spreadSheetName);
        SpreadsheetEntry spreadsheet = feed.getEntries().get(spreadsheetIndex);

        // Get the worksheet to load
        if (spreadsheet.getWorksheets().size() == 1) {
            setListFeedUrl(spreadsheet.getWorksheets().get(0).getListFeedUrl());
        } else {
            List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
            int worksheetIndex = getWorksheetIndex(worksheets, workSheetName);
            WorksheetEntry worksheet = (WorksheetEntry) worksheets.get(worksheetIndex);
            setListFeedUrl(worksheet.getListFeedUrl());
        }
        
        return getService().getFeed(getListFeedUrl(), ListFeed.class);
    }
    
    /**
     * Lists all rows in the spreadsheet.
     * 
     * @throws ServiceException when the request causes an error in the Google
     *         Spreadsheets service.
     * @throws IOException when an error occurs in communication with the Google
     *         Spreadsheets service.
     */
    public void cacheAllEntries() throws IOException, ServiceException {
        ListFeed feed = getService().getFeed(getListFeedUrl(), ListFeed.class);

        for (ListEntry entry : feed.getEntries()) {
            readAndCacheEntry(entry);
        }
    }

    /**
     * Prints the entire list entry, in a way that mildly resembles what the
     * actual XML looks like.
     * 
     * In addition, all printed entries are cached here. This way, they can be
     * updated or deleted, without having to retrieve the version identifier again
     * from the server.
     * 
     * @param entry the list entry to print
     */
    public void readAndCacheEntry(ListEntry entry) {
        
        if(entriesCached == null) {
            entriesCached = Maps.newHashMap();
        }

        // We only care about the entry id, chop off the leftmost part.
        // I.E., this turns http://spreadsheets.google.com/..../cpzh6 into cpzh6.
        String id = entry.getId().substring(entry.getId().lastIndexOf('/') + 1);

        // Cache all displayed entries so that they can be updated later.
        entriesCached.put(id, entry);

    }

    /**
     * Extracts all entries from ListFeed and converts each entry to the map.
     * Map<String, Object>.
     * 
     * @param feed
     * @return List of entry maps.
     */
    public List<Map<String, Object>> getListOfMapsFromListFeed(ListFeed feed) {
        
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        
        for (ListEntry entry : feed.getEntries()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for(String tag : entry.getCustomElements().getTags()){
                map.put(tag, entry.getCustomElements().getValue(tag));
            }
            map.put(REC_NUMBER_NAME, new Integer(list.size()));
            list.add(map);
        }        
        return list;
    }
    
    public List<Map<String, Object>> getListOfMapsFromListFeed(List<ListEntry> listEntries) {
        
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(); 
        for (ListEntry entry : listEntries) {
            Map<String, Object> map = new HashMap<String, Object>();
            for(String tag : entry.getCustomElements().getTags()){
                map.put(tag, entry.getCustomElements().getValue(tag));
            }
            list.add(map);
        }        
        return list;
    }
    
    
    public List<Map<String, Object>> getListOfMapsFromListFeed(List<ListEntry> listEntries, int start, int end) {
        
        System.out.println("START: " + start + "; END: " + end);
        
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(); 
        for (int i = start; i <= end; i++) {
            ListEntry entry = listEntries.get(i);
            Map<String, Object> map = new HashMap<String, Object>();
            for(String tag : entry.getCustomElements().getTags()){
                map.put(tag, entry.getCustomElements().getValue(tag));
            }
            
            System.out.println("map from ListEntry: " + map);
            
            list.add(map);
        }        
        return list;
    }
  
    /**
     * Returns spreadsheet index by SpreadSheet name
     * 
     * @param spreadsheets
     * @param spreadSheetName
     * @return spreadsheet index
     */
    private int getSpreadsheetIndex(List<SpreadsheetEntry> spreadsheets, String spreadSheetName) {
        int index = 0;
        
        String itemNameTrimmed = spreadSheetName.trim();
        for(SpreadsheetEntry item : spreadsheets){
            if(item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) {
                break;
            }
            index++;
        }
        return index;
    }

    /**
     * Returns index from worksheet list by workSheetName
     * 
     * @param worksheets
     * @param workSheetName
     * @return workSheet index
     */
    private int getWorksheetIndex(List<WorksheetEntry> worksheets, String workSheetName) {
        int index = 0;
        
        String itemNameTrimmed = workSheetName.trim();
        for(WorksheetEntry item : worksheets){
            if(item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) {
                break;
            }
            index++;
        }
        return index;
    }
      
    /**
     * Searches rows with a full text search string, finding any rows that match
     * all the given words.
     * 
     * @param fullTextSearchString a string like "Rosa 555" will look for the
     *        substrings Rosa and 555 to appear anywhere in the row
     * @throws ServiceException when the request causes an error in the Google
     *         Spreadsheets service.
     * @throws IOException when an error occurs in communication with the Google
     *         Spreadsheets service.
     */
    public List<Map<String, Object>> search(String fullTextSearchString) throws IOException,
            ServiceException {
        
        ListFeed feed;        
        if(fullTextSearchString == null || fullTextSearchString.isEmpty()){
            feed = getService().getFeed(getListFeedUrl(), ListFeed.class);
        } else {
            ListQuery query = new ListQuery(getListFeedUrl());
            query.setFullTextQuery(fullTextSearchString);
            feed = getService().query(query, ListFeed.class);
        }        
        List<Map<String, Object>> list = getListOfMapsFromListFeed(feed);
        
        return list;
    }

    /**
     * Performs a full database-like query on the rows.
     * 
     * @param structuredQuery a query like: name = "Bob" and phone != "555-1212"
     * @throws ServiceException when the request causes an error in the Google
     *         Spreadsheets service.
     * @throws IOException when an error occurs in communication with the Google
     *         Spreadsheets service.
     */
    public List<Map<String, Object>> endOfSheetQuery(String structuredQuery) throws IOException,
            ServiceException {
        
        ListFeed feed;        
        if(structuredQuery == null || structuredQuery.isEmpty()){
            feed = getService().getFeed(getListFeedUrl(), ListFeed.class);
        } else {
            StringBuilder queryString = new StringBuilder();
            
            for(String field : structuredQuery.split(",")){
                if(queryString.length() > 0){
                    queryString.append(" or ");                            
                }
                queryString.append(field).append(" != null");                        
            }
            
            ListQuery query = new ListQuery(getListFeedUrl());
            query.setSpreadsheetQuery(queryString.toString());
            feed = getService().query(query, ListFeed.class);
        }        
        List<Map<String, Object>> list = getListOfMapsFromListFeed(feed);
        
        return list;
    }

    public ListEntry updateEntryValues(ListEntry entry, String nameValuePairs) throws ServiceException, ServiceException, ServiceException, IOException {
        
        assignEntryValues(entry, nameValuePairs);
        return entry;
    }

    
    public ListEntry updateEntryValues(ListEntry entry, Map<String, Object> map, String keyList) throws ServiceException, ServiceException, ServiceException, IOException {
        
        assignEntryValues(entry, map, keyList);
        return entry;
    }
    
    public ListEntry updateEntryFormulas(ListEntry entry, String nameValuePairs, String formulaList) 
            throws ServiceException, ServiceException, ServiceException, IOException {
        
        applyEntryFormulas(entry, nameValuePairs, formulaList);
        return entry;
    }
 
    public ListEntry updateEntryFormulas(ListEntry entry, Map<String, Object> nameValuePairs, String formulaList) 
            throws ServiceException, ServiceException, ServiceException, IOException {
        
        applyEntryFormulas(entry, nameValuePairs, formulaList);
        return entry;
    }

    public ListEntry addNewEntryValues(String nameValuePairs) throws IOException, ServiceException {
        ListEntry entry = new ListEntry();
        assignEntryValues(entry, nameValuePairs);
        
        return entry;
    }

    public ListEntry addNewEntryValues(Map<String, Object> nameValuePairs, String keyList) throws IOException, ServiceException {
        ListEntry entry = new ListEntry();
        assignEntryValues(entry, nameValuePairs, keyList);
        
        return entry;
    }

    
    public ListEntry addNewEntryFormulas(ListEntry entry, String nameValuePairs, String formulaList) throws IOException, ServiceException {
       
        applyEntryFormulas(entry, nameValuePairs, formulaList);

        return entry;
    }

    private ListEntry applyEntryFormulas(ListEntry entry, String nameValuePairs, String formulaList) throws IOException, ServiceException {
        
        // Split first by the commas between the different fields.
        for (String nameValuePair : formulaList.split(",")) {
            // Then, split by the equal sign.
            String[] parts = nameValuePair.split("=", 2);
            String tag = parts[0].trim(); // such as "name"
            String value;
            try{
                value = parts[1].trim(); // such as "Fred"
            } catch(Exception e){
                value = null;
            }
            if (value != null) {
                entry.getCustomElements().setValueLocal(tag, evaluateCellValue(nameValuePairs, value));
            } else {
                entry.getCustomElements().setValueLocal(tag, value);
            }
        }
        return entry;
    }      
    
    
    private ListEntry applyEntryFormulas(ListEntry entry, Map<String, Object> nameValuePairs, String formulaList) throws IOException, ServiceException {
        
        // Split first by the commas between the different fields.
        for (String nameValuePair : formulaList.split(",")) {
            // Then, split by the equal sign.
            String[] parts = nameValuePair.split("=", 2);
            String tag = parts[0].trim(); // such as "name"
            String value;
            try{
                value = parts[1].trim(); // such as "Fred"
            } catch(Exception e){
                value = null;
            }
            if (value != null) {
                entry.getCustomElements().setValueLocal(tag, evaluateCellValue(nameValuePairs, value));
            } else {
                entry.getCustomElements().setValueLocal(tag, value);
            }
        }
        return entry;
    }
    
    private String evaluateCellValue(String nameValuePairs, String formula) {

        Binding binding = new Binding();
        for (String nameValuePair : nameValuePairs.split(",")) {
            String[] parts  = nameValuePair.split("=", 2);
            Object obj      = convert(parts[1].trim());
            binding.setVariable(parts[0].trim(), obj);
        }        
        
        GroovyShell shell = new GroovyShell(binding);
        
        String result = shell.evaluate(formula).toString();

        return result;
    }
    
    private String evaluateCellValue(Map<String, Object> nameValuePairs, String formula) {

        Binding binding = new Binding();
        for (Entry entry : nameValuePairs.entrySet()) {            
            binding.setVariable(((String)entry.getKey()).trim(), entry.getValue());
        }        
        
        GroovyShell shell = new GroovyShell(binding);
        
        String result = shell.evaluate(formula).toString();

        return result;
    }
    
    /**
     * Populates ListEntry entry with data from nameValuesPairs.
     * String has format: name1=value1,name2=value2, ....
     * @param entry
     * @param nameValuePairs 
     */
    private void assignEntryValues(ListEntry entry, String nameValuePairs) {
       
        // Split first by the commas between the different fields.
        for (String nameValuePair : nameValuePairs.split("&")) {
            // Then, split by the equal sign.
            String[] parts = nameValuePair.split("=", 2);
            String tag = parts[0].trim(); // such as "name"
            String value = parts[1].trim(); // such as "Fred"

            entry.getCustomElements().setValueLocal(tag, value);
        }
    }

    
    private void assignEntryValues(ListEntry listEntry, Map<String, Object> map, String keyList) {
        Map<String, Boolean> keyMap = Maps.newHashMap();
        if (keyList != null) {
            for (String key : keyList.split(",")) {
                keyMap.put(key, Boolean.TRUE);
            }
        } else {
            keyMap = null;
        }
        for(Entry entry : map.entrySet()){
            if(entry.getKey() == null) {
                continue;
            }
            if (keyMap != null && keyMap.size() > 0) {
                if (keyMap.containsKey((String) entry.getKey()) && entry.getValue() != null) {
                    listEntry.getCustomElements().setValueLocal((String) entry.getKey(), entry.getValue().toString());
                }
            } else {
                if (entry.getValue() != null) {
                    listEntry.getCustomElements().setValueLocal((String) entry.getKey(), entry.getValue().toString());
                }
            }
        }
    }
    
    /**
     * @return the service
     */
    public SpreadsheetService getService() {
        return service;
    }

    /**
     * @param service the service to set
     */
    public void setService(SpreadsheetService service) {
        this.service = service;
    }

    /**
     * @return the listFeedUrl
     */
    public URL getListFeedUrl() {
        return listFeedUrl;
    }

    /**
     * @param listFeedUrl the listFeedUrl to set
     */
    public void setListFeedUrl(URL listFeedUrl) {
        this.listFeedUrl = listFeedUrl;
    }

}
