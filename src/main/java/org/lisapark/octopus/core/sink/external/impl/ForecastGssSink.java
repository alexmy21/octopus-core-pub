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
package org.lisapark.octopus.core.sink.external.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.util.ServiceException;
import com.google.gson.Gson;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.util.gss.GssListUtils;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
@Persistable
public class ForecastGssSink extends AbstractNode implements ExternalSink {
    
     private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ForecastGssSink.class.getName());    
    
    private static final String DEFAULT_NAME        = "Forecast to GSS";
    private static final String DEFAULT_DESCRIPTION = "Output Forecast to Google Spreadsheet.";
    
    private static final int USER_EMAIL_PARAMETER_ID        = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int WORK_BOOK_PARAMETER_ID         = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID      = 4;
    private static final int START_ROW_PARAMETER_ID         = 5;
    private static final int ROW_INDEX_NAME_PARAMETER_ID    = 6;
    private static final int OUTPUT_FIELD_LIST_PARAMETER_ID = 7;
    private static final int FORMULA_NAME_PARAMETER_ID      = 8;
    private static final int FORECAST_NAME_PARAMETER_ID     = 9;
    private static final int JSON_OBJECT_NAME_PARAMETER_ID  = 10;
    private static final int FORECAST_HORIZON_PARAMETER_ID  = 11;
    
    private static final String USER_EMAIL          = "User email: ";
    private static final String PASSWORD            = "Password: ";
    private static final String WORKBOOK            = "Spreadsheet name: ";
    private static final String SPREAD_SHEET        = "Sheet name: ";
    private static final String START_ROW_NAME      = "Start Row for output: ";
    private static final String ROW_INDEX_NAME      = "Row index field name: ";
    private static final String OUTPUT_FIELD_LIST   = "Output field list: ";
    private static final String FORMULA_NAME        = "Forecast Formula field name: ";
    private static final String FORCAST_NAME        = "Forecast Result field name: ";
    private static final String JSON_OBJECT_NAME    = "JSON Object field name: ";
    private static final String FORECAST_HORIZON    = "Forecast Horizon: ";
    
    private static final String DEFAULT_INPUT       = "Input data:";
    
    private static final Map<Integer, String> paramMap    = new HashMap<Integer, String>();

    private Input<Event> input;

    private ForecastGssSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ForecastGssSink(UUID id, ForecastGssSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }
  
    public String getUserEmail() {
        return getParameter(USER_EMAIL_PARAMETER_ID).getValueAsString();
    }
   
    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }
   
    public String getWorkBook() {
        return getParameter(WORK_BOOK_PARAMETER_ID).getValueAsString();
    }
   
    public String getSpreadSheet() {
        return getParameter(SPREAD_SHEET_PARAMETER_ID).getValueAsString();
    }
   
    public int getStartRow() {
        return getParameter(START_ROW_PARAMETER_ID).getValueAsInteger();
    }
    
    public String getIndexFieldName() {
        return getParameter(ROW_INDEX_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getOutFieldList() {
        return getParameter(OUTPUT_FIELD_LIST_PARAMETER_ID).getValueAsString();
    }

    public String getFormulaName() {
        return getParameter(FORMULA_NAME_PARAMETER_ID).getValueAsString();
    }
   
    public String getForecastName() {
        return getParameter(FORECAST_NAME_PARAMETER_ID).getValueAsString();
    }
    
    public String getJsonObjectName() {
        return getParameter(JSON_OBJECT_NAME_PARAMETER_ID).getValueAsString();
    }
    
    public int getForecastHorizon() {
        return getParameter(FORECAST_HORIZON_PARAMETER_ID).getValueAsInteger();
    }
    
    private ForecastGssSink(ForecastGssSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
  
    public Input<Event> getInput() {
        return input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean isConnectedTo(Source source) {

        return input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (input.isConnectedTo(source)) {
            input.clearSource();
        }
    }

    @Override
    public ForecastGssSink newInstance() {
        return new ForecastGssSink(UUID.randomUUID(), this);
    }

    @Override
    public ForecastGssSink copyOf() {
        return new ForecastGssSink(this);
    }

    public static ForecastGssSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        ForecastGssSink gssSink = new ForecastGssSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, USER_EMAIL)
                .defaultValue("demo1@lisa-park.com")
                .required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD)
                .defaultValue("isasdemo")
                .required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, WORKBOOK)
                .defaultValue("USFutures15")
                .required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, SPREAD_SHEET)
                .defaultValue("Forecast")
                .required(true));
        
        gssSink.addParameter(Parameter.integerParameterWithIdAndName(START_ROW_PARAMETER_ID, START_ROW_NAME).required(true)
                .defaultValue(0)
                .description("Start row number for output data. Use zero, if you want enter output data to the new row."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(ROW_INDEX_NAME_PARAMETER_ID, ROW_INDEX_NAME).required(true)
                .defaultValue("index")
                .description("Field name that holds current row index."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(OUTPUT_FIELD_LIST_PARAMETER_ID, OUTPUT_FIELD_LIST).required(false)
                .defaultValue("")
                .description("Comma separated field list."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(FORMULA_NAME_PARAMETER_ID, FORMULA_NAME).required(false)
                .defaultValue("formula")
                .description("Name of the Attribute that holds Forecast formula."));
         
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(FORECAST_NAME_PARAMETER_ID, FORCAST_NAME).required(false)
                .defaultValue("forecast")
                .description("Name of the Attribute that holds Forecast Resulr."));
        
        gssSink.addParameter(Parameter.integerParameterWithIdAndName(FORECAST_HORIZON_PARAMETER_ID, FORECAST_HORIZON).required(true)
                .defaultValue(1)
                .description("Set Forecast Horizon."));        
         
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(JSON_OBJECT_NAME_PARAMETER_ID, JSON_OBJECT_NAME).required(false)
                .defaultValue("JSON")
                .description("Name of the Attribute that holds Forecast JSON Object as a string."));
        
        return gssSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledGssSink(copyOf());
    }

    static class CompiledGssSink extends CompiledExternalSink {
        
        private final ForecastGssSink gssSink;
        
        GssListUtils gssList;
        private ListFeed listFeed;
        
        protected CompiledGssSink(ForecastGssSink gssSink) {
            super(gssSink);            
            this.gssSink = gssSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
                        
            if (event != null) {
                try {  
                    if (gssList == null) {
                        gssList = new GssListUtils(
                                DEFAULT_NAME, gssSink.getWorkBook(), gssSink.getSpreadSheet(),
                                gssSink.getUserEmail(), gssSink.getPassword());
                        setListFeed(gssList.loadSheet());
                    }
                                         
                    listFeed = getListFeed();
                    List<ListEntry> list    = listFeed.getEntries();                    
                    String keyList          = gssSink.getOutFieldList();                    
                    Map<String, Object> map = event.getData();
                    
                    String formulaName      = gssSink.getFormulaName().isEmpty() ? null : gssSink.getFormulaName();
                    String forecastName     = gssSink.getForecastName().isEmpty() ? null : gssSink.getForecastName();
                    String jsonName         = gssSink.getJsonObjectName().isEmpty() ? null : gssSink.getJsonObjectName();
                    String rowIndexName     = gssSink.getIndexFieldName();
                    
                    int rowIndex = 0;
                    if(map.containsKey(rowIndexName)){
                        // Get row index if it is possible
                        try{
                            rowIndex = Integer.parseInt(map.get(rowIndexName).toString());
                        } catch(Exception e){
                            logger.log(Level.INFO, "Error getting row index value.");    
                        }
                    }
                    
                    // Add calculated Forecast to the event data map
                    HashMap<String, Object> jsonMap = null;
                    if (formulaName != null && jsonName != null) {
                        Gson gson = new Gson();
                        String jsonString = (String) map.get(jsonName);
                        
                        if (jsonString != null && !jsonString.isEmpty()) {
                            jsonMap = Maps.newHashMap(gson.fromJson(jsonString, HashMap.class));

                            logger.log(Level.INFO, "JSON MAP: ==> {0}", jsonMap);

                            map.putAll(jsonMap);                            
                            String formula = jsonMap.get(formulaName).toString();

                            logger.log(Level.INFO, "FORMULA: ==> {0}", formula);
                            
                            map.put(formulaName, formula);                            
                            map.remove(jsonName);
                            //
                            Object result = evaluateFormula(map, formula);
                            map.put(forecastName, result);
                        }                        
                    }
 
logger.log(     Level.INFO, "event.getData(): ==> {0}", map);                    
                    
                    Integer startRow = gssSink.getStartRow() == 0 ? list.size() - 1 : gssSink.getStartRow();                    
                    
                    String newEntry;
                    if(keyList.isEmpty()){
                        newEntry = getGssListEntryFromEvent(map);
                    } else {
                        newEntry = getGssListEntryFromEvent(map, keyList);
                    }                    
                    editEntryList(list, startRow, rowIndex, newEntry);
                    
                    // Calculate Forecast values
                    if (jsonMap != null) {
                        int horizon = gssSink.getForecastHorizon();
                        calculateForecast(list, jsonMap, startRow, rowIndex + 1, horizon);
                    }
                    
                    ctx.getStandardOut().println(newEntry);

                } catch (ServiceException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (IOException ex) {
                    Exceptions.printStackTrace(ex);
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }
        
        /**
         * 
         * @param list
         * @param startRow
         * @param i 
         */
        private void calculateForecast(List<ListEntry> list, HashMap<String, Object> jsonMap, 
                Integer startRow, int rowIndex, int horizon) 
                throws IOException, ServiceException {
            String formulaName  = gssSink.getFormulaName();
            String formula      = jsonMap.get(formulaName).toString();
            String indexName    = gssSink.getIndexFieldName();
            
            for (int i = rowIndex; i < rowIndex + horizon; i++) {
                HashMap<String, Object> map = Maps.newHashMap();
                map.put(indexName, i);
                
                String newEntry = gssSink.getForecastName() + "="
                        + evaluateFormula(map, formula);
                
                if (list.size() > startRow + rowIndex) {

                    ListEntry entry = list.get(startRow + rowIndex);

                    // Split first by the commas between the different fields.
                    for (String nameValuePair : newEntry.split(",")) {
                        // Then, split by the equal sign.
                        String[] parts = nameValuePair.split("=", 2);
                        String tag = parts[0].trim(); // such as "name"
                        String value = parts[1].trim(); // such as "Fred"

                        entry.getCustomElements().setValueLocal(tag, value);
                    }

                    entry.update();
                } else {
                    ListEntry entry = gssList.addNewEntryValues(newEntry);
                    gssList.getService().insert(gssList.getListFeedUrl(), entry);
                }
            }
        }


        /**
         * 
         * @param list
         * @param startRow
         * @param rowIndex
         * @param newEntry
         * @throws ServiceException
         * @throws IOException 
         */
        private void editEntryList(List<ListEntry> list, Integer startRow, int rowIndex, String newEntry) throws ServiceException, IOException {
            if(list.size() > startRow + rowIndex){
                ListEntry entry = list.get(startRow + rowIndex);

                // Split first by the commas between the different fields.
                for (String nameValuePair : newEntry.split(",")) {
                    // Then, split by the equal sign.
                    String[] parts = nameValuePair.split("=", 2);
                    String tag = parts[0].trim(); // such as "name"
                    String value = parts[1].trim(); // such as "Fred"

                    entry.getCustomElements().setValueLocal(tag, value);
                }

                entry.update();
            } else {
                ListEntry entry = gssList.addNewEntryValues(newEntry);
                gssList.getService().insert(gssList.getListFeedUrl(), entry);                        
            }
        }
        
        /**
         * 
         * @param nameValuePairs
         * @param formula
         * @return 
         */
        private String evaluateFormula(Map<String, Object> nameValuePairs, String formula) {

            Binding binding = new Binding();
            for (Entry entry : nameValuePairs.entrySet()) {
                binding.setVariable(((String) entry.getKey()).trim(), entry.getValue());
            }
            GroovyShell shell = new GroovyShell(binding);
            String result = shell.evaluate(formula).toString();
            return result;
        }
        
        /**
         * 
         * @param map
         * @param keyList
         * @return 
         */
        private String getGssListEntryFromEvent(Map<String, Object> map, String keyList) {
            
            if(keyList.trim().isEmpty()){
                return getGssListEntryFromEvent(map);
            }
            String[] keys = keyList.split(",");
            Set<String> keySet = Sets.newHashSet();            
            keySet.addAll(Arrays.asList(keys));            
            StringBuilder builder = new StringBuilder();  
            
            for (Entry<String, Object> entry : map.entrySet()) {
                String entryValueString;
                String entryKeyString;

                // Use recursion to collect all entries from nested maps
                if (entry.getValue() instanceof Map) {
                    entryValueString = getGssListEntryFromEvent((Map<String, Object>) entry.getValue(), keyList);
                    
                    logger.log(     Level.INFO, "entryValueString: ==> {0}", entryValueString);
                    
                    if (builder.length() > 0) {
                        builder.append(",");
                    }
                    builder.append(entryValueString);
                    entryKeyString = "";
                } else {
                    entryValueString    = entry.getValue().toString();
                    entryKeyString      = entry.getKey().toString();
                }

                if (keySet.contains(entryKeyString)) {
                    if (builder.length() > 0) {
                        builder.append(",");
                    }
                    if (entryKeyString.isEmpty()) {
                        builder.append(entryValueString);
                    } else {
                        builder.append(entryKeyString).append("=").append(entryValueString);
                    }
                }
            }
            
logger.log(     Level.INFO, "builder.toString(): ==> {0}", builder.toString());
            
            return builder.toString();
        }

        /**
         * 
         * @param map
         * @return 
         */
        private String getGssListEntryFromEvent(Map<String, Object> map) {
            StringBuilder builder = new StringBuilder();  
            
            for (Entry<String, Object> entry : map.entrySet()) {
                String entryValueString;
                String entryKeyString;

                // Use recursion to collect all entries from nested maps
                if (entry.getValue() instanceof Map) {
                    entryValueString = getGssListEntryFromEvent((Map<String, Object>) entry.getValue());
                    entryKeyString = "";
                } else {
                    entryValueString = entry.getValue().toString();
                    entryKeyString = entry.getKey().toString();
                }

                if (builder.length() > 0) {
                    builder.append(",");
                }
                if (entryKeyString.isEmpty()) {
                    builder.append(entryValueString);
                } else {
                    builder.append(entryKeyString).append("=").append(entryValueString);
                }
            }
            
logger.log(     Level.INFO, "builder.toString(): ==> {0}", builder.toString());
            
            return builder.toString();
        }
        
        /**
         * @return the listFeed
         */
        public ListFeed getListFeed() {
            return listFeed;
        }

        /**
         * @param listFeed the listFeed to set
         */
        public void setListFeed(ListFeed listFeed) {
            this.listFeed = listFeed;
        }

    }
}
