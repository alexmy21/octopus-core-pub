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
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.util.ServiceException;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
public class GssSink_old extends AbstractNode implements ExternalSink {

    private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(GssSink_old.class.getName());
    
    private static final String DEFAULT_NAME                    = "Output to GSS";
    private static final String DEFAULT_DESCRIPTION             = "Output to Google Spreadsheet.";
    
    private static final int USER_EMAIL_PARAMETER_ID            = 1;
    private static final int PASSWORD_PARAMETER_ID              = 2;
    private static final int WORK_BOOK_PARAMETER_ID             = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID          = 4;
    private static final int START_ROW_PARAMETER_ID             = 5;
    private static final int ROW_INDEX_NAME_PARAMETER_ID        = 6;
    private static final int OUTPUT_FIELD_LIST_PARAMETER_ID     = 7;
    private static final int FORMULA_LIST_PARAMETER_ID          = 8;
    
    private static final String USER_EMAIL          = "User email: ";
    private static final String PASSWORD            = "Password: ";
    private static final String WORKBOOK            = "Spreadsheet name: ";
    private static final String SPREAD_SHEET        = "Sheet name: ";
    private static final String START_ROW_NAME      = "Start Row for output: ";
    private static final String ROW_INDEX_NAME      = "Row index field name: ";
    private static final String OUTPUT_FIELD_LIST   = "Output field list: ";
    private static final String FORMULA_LIST        = "Formula list: ";
    private static final String DEFAULT_INPUT       = "Input data:";
    
//    private static final Map<Integer, String> paramMap    = new HashMap<Integer, String>();
    private Input<Event> input;

    private GssSink_old(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private GssSink_old(UUID id, GssSink_old copyFromNode) {
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

    public String getFormulaList() {
        return getParameter(FORMULA_LIST_PARAMETER_ID).getValueAsString();
    }

    private GssSink_old(GssSink_old copyFromNode) {
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
    public GssSink_old newInstance() {
        return new GssSink_old(UUID.randomUUID(), this);
    }

    @Override
    public GssSink_old copyOf() {
        return new GssSink_old(this);
    }

    public static GssSink_old newTemplate() {
        UUID sinkId = UUID.randomUUID();

        GssSink_old gssSink = new GssSink_old(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

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
                .defaultValue("")
                .required(true));

//        gssSink.addParameter(Parameter.integerParameterWithIdAndName(START_ROW_PARAMETER_ID, START_ROW_NAME).required(true)
//                .defaultValue(0)                
//                .description("Start row number for output data. Use zero, if you want enter output data to the new row."));
//        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(ROW_INDEX_NAME_PARAMETER_ID, ROW_INDEX_NAME).required(true)
                .defaultValue("index")
                .description("Field name that holds current row index."));

        gssSink.addParameter(Parameter.stringParameterWithIdAndName(OUTPUT_FIELD_LIST_PARAMETER_ID, OUTPUT_FIELD_LIST).required(false)
                .defaultValue("")
                .description("Comma separated field list."));

        gssSink.addParameter(Parameter.stringParameterWithIdAndName(FORMULA_LIST_PARAMETER_ID, FORMULA_LIST).required(false)
                .defaultValue("")
                .description("Comma separated formula list. Example:"
                + " TOTAL1=PRICE1*QTY1,AVE=(TOTAL1+TOTAL2+TOTAL3)/3."
                + " Here: TOTAL1, AVE, TOTAL2, TOTAL3, QTY1 - corresponding column names,"
                + " from the sheet first row."));

        return gssSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledGssSink(copyOf());
    }

    static class CompiledGssSink extends CompiledExternalSink {

        private final GssSink_old gssSink;
        GssListUtils gssList;
        private ListFeed listFeed;

        protected CompiledGssSink(GssSink_old gssSink) {
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

//                    listFeed = getListFeed();
//                    List<ListEntry> list = listFeed.getEntries();

                    String outFieldNameList = gssSink.getOutFieldList();

                    // Extract data from event
                    //==========================================================
                    Map<String, Object> map = expendEventMap(event, gssSink.getOutFieldList());

                    String formulaList = gssSink.getFormulaList().isEmpty() ? null : gssSink.getFormulaList();
                    String rowIndexName = gssSink.getIndexFieldName();

                    int rowIndex = 0;
                    if (map.containsKey(rowIndexName)) {
                        // Get row index if it is possible
                        try {
                            rowIndex = Integer.parseInt(map.get(rowIndexName).toString());
                        } catch (Exception e) {
                            logger.log(Level.INFO, "Error getting row index value.");
                        }
                    }

                    // Add calculated fields to the event data map
                    //==========================================================
                    if (formulaList != null) {
                        String[] formulas = formulaList.split(",");

                        int length = formulas.length;
                        for (int i = 0; i < length; i++) {
                            String[] nameFormula = formulas[i].split("=");
                            String name = nameFormula[0];
                            String formula = nameFormula[1];

                            Object result = evaluateFormula(map, formula);

                            map.put(name, result);
                        }
                    }

                    logger.log(Level.INFO, "event.getData(): ==> {0}", map);

                    // startRow assignment is temporarelly disabled
                    // Looking for the final solution
//                    Integer startRow = 0; //gssSink.getStartRow();

                    // Add new entry to the Google spreadsheet
                    //==========================================================
                    ListEntry entry;
                    
//                    if(startRow == 0){
                    
                    entry = gssList.addNewEntryValues(map, outFieldNameList);
                    gssList.getService().insert(gssList.getListFeedUrl(), entry);
                    
//                    } else if(list.size() > startRow + rowIndex){
//                        entry = list.get(startRow + rowIndex);
//                        entry = gssList.updateEntryValues(entry, map, outFieldNameList);                       
//                        entry.update();
//                    } else {
//                        entry = gssList.addNewEntryValues(map, outFieldNameList);
//                        gssList.getService().insert(gssList.getListFeedUrl(), entry);                        
//                    }
                    
                    ctx.getStandardOut().println(map);

                } catch (ServiceException ex) {
                    Exceptions.printStackTrace(ex);
                } catch (IOException ex) {
                    Exceptions.printStackTrace(ex);
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private Map<String, Object> expendEventMap(Event event, String attributeList) {

            Map<String, Object> data = event.getData();
            Map<String, Object> outputMap = Maps.newHashMap();
            String[] attList = attributeList.split(",");

            for (int i = 0; i < attList.length; i++) {
                String attr = attList[i];

                if (data.get(attr) != null) {
                    if (data.get(attr) instanceof Map) {
                        outputMap.put(attr, data.get(attr).toString());
                        Map map = Maps.newHashMap((Map) data.get(attr));
                        outputMap = extractMap(map, attr, outputMap);
                    } else {
                        outputMap.put(attr, data.get(attr));
                    }
                }
            }

            if (outputMap.isEmpty()) {
                return null;
            } else {
                return outputMap;
            }
        }

        public Map<String, Object> extractMap(Map<String, Object> map, String attr, Map<String, Object> outputMap) {

            for (Entry entry : map.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    outputMap.put((String) entry.getKey(), entry.getValue().toString());
                    Map _map = Maps.newHashMap((Map) entry.getValue());
                    outputMap = extractMap(_map, entry.getKey().toString(), outputMap);
                } else {
                    outputMap.put((String) entry.getKey(), entry.getValue());
                }
            }

            return outputMap;
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
