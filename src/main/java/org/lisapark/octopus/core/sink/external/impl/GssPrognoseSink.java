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
import com.google.common.collect.Sets;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.util.ServiceException;
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
public class GssPrognoseSink extends AbstractNode implements ExternalSink {
    
     private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssPrognoseSink.class.getName());    
    
    private static final String DEFAULT_NAME        = "Таблица Google";
    private static final String DEFAULT_DESCRIPTION = "Вывод результатов в таблицу Google";
    
    private static final int USER_EMAIL_PARAMETER_ID        = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int WORK_BOOK_PARAMETER_ID         = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID      = 4;   
    private static final int OUTPUT_FIELD_LIST_PARAMETER_ID = 6;
    private static final int FORMULA_LIST_PARAMETER_ID      = 7;
    
    private static final int FORECAST_PARAMETER_ID          = 8;
    private static final int TIME_LINE_FIELD_PARAMETER_ID   = 9;
    
    private static final String USER_EMAIL          = "email Пользователя: ";
    private static final String PASSWORD            = "Пароль: ";
    private static final String WORKBOOK            = "Имя Эл. таблицы: ";
    private static final String SPREAD_SHEET        = "Имя таблицы: ";
    
    private static final String OUTPUT_FIELD_LIST   = "Список выходных полей: ";
    private static final String FORMULA_LIST        = "Список формул: ";
    
    private static final String FORECAST            = "Глубина прогнзирования: ";
    private static final String TIME_LINE_FIELD     = "Имя поля для времени: ";
    
    private static final String DEFAULT_INPUT       = "Входные данные";
    
    private static Map<Integer, String> paramMap    = new HashMap<Integer, String>();
    

    private Input<Event> input;

    private GssPrognoseSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private GssPrognoseSink(UUID id, GssPrognoseSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }
    
    
    @SuppressWarnings("unchecked")
    public void setUserEmail(String userEmail) throws ValidationException {
        getParameter(USER_EMAIL_PARAMETER_ID).setValue(userEmail);
    }

    public String getUserEmail() {
        return getParameter(USER_EMAIL_PARAMETER_ID).getValueAsString();
    }
    
     
    @SuppressWarnings("unchecked")
    public void setPassword(String password) throws ValidationException {
        getParameter(PASSWORD_PARAMETER_ID).setValue(password);
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }
     
    @SuppressWarnings("unchecked")
    public void setWorkBook(String workBook) throws ValidationException {
        getParameter(WORK_BOOK_PARAMETER_ID).setValue(workBook);
    }

    public String getWorkBook() {
        return getParameter(WORK_BOOK_PARAMETER_ID).getValueAsString();
    }
     
    @SuppressWarnings("unchecked")
    public void setSpreadSheet(String spreadSheet) throws ValidationException {
        getParameter(SPREAD_SHEET_PARAMETER_ID).setValue(spreadSheet);
    }

    public String getSpreadSheet() {
        return getParameter(SPREAD_SHEET_PARAMETER_ID).getValueAsString();
    }
     
    @SuppressWarnings("unchecked")
    public void setOutFieldList(String outFieldList) throws ValidationException {
        getParameter(OUTPUT_FIELD_LIST_PARAMETER_ID).setValue(outFieldList);
    }

    public String getOutFieldList() {
        return getParameter(OUTPUT_FIELD_LIST_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setFormulaList(String outFieldList) throws ValidationException {
        getParameter(FORMULA_LIST_PARAMETER_ID).setValue(outFieldList);
    }

    public String getFormulaList() {
        return getParameter(FORMULA_LIST_PARAMETER_ID).getValueAsString();
    }
    
    @SuppressWarnings("unchecked")
    public void setForecast(String forecast) throws ValidationException {
        getParameter(FORECAST_PARAMETER_ID).setValue(forecast);
    }

    public int getForecast() {
        return getParameter(FORECAST_PARAMETER_ID).getValueAsInteger();
    }
    
    @SuppressWarnings("unchecked")
    public void setTimeLineField(String timelineField) throws ValidationException {
        getParameter(TIME_LINE_FIELD_PARAMETER_ID).setValue(timelineField);
    }

    public String getTimeLineField() {
        return getParameter(TIME_LINE_FIELD_PARAMETER_ID).getValueAsString();
    }
    
    private GssPrognoseSink(GssPrognoseSink copyFromNode) {
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
    public GssPrognoseSink newInstance() {
        return new GssPrognoseSink(UUID.randomUUID(), this);
    }

    @Override
    public GssPrognoseSink copyOf() {
        return new GssPrognoseSink(this);
    }

    public static GssPrognoseSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        GssPrognoseSink gssSink = new GssPrognoseSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, USER_EMAIL).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, WORKBOOK).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, SPREAD_SHEET).required(true));
       
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(OUTPUT_FIELD_LIST_PARAMETER_ID, OUTPUT_FIELD_LIST).required(false)
                .defaultValue("")
                .description("Перечень разделенных запятыми полей, которые должны быть выведены в эектронную таблицу."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(FORMULA_LIST_PARAMETER_ID, FORMULA_LIST).required(false)
                .defaultValue("")
                .description("Перечень разделенных запятыми полей, которые содержат формулы. Формат записи:"
                + " TOTAL1=PRICE1*QTY1,AVE=(TOTAL1+TOTAL2+TOTAL3)/3."
                + " Здесь TOTAL1, AVE, TOTAL2, TOTAL3, QTY1 - имена соответствующих колонок,"
                + " которые записаны в первой сроке электронной таблицы."));
        
        
        gssSink.addParameter(Parameter.integerParameterWithIdAndName(FORECAST_PARAMETER_ID, FORECAST).required(true)
                .defaultValue(0)
                .description("Глубина прогнозирования указывает количство временных интервалов, на которые рассчитывается прогноз."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(TIME_LINE_FIELD_PARAMETER_ID, TIME_LINE_FIELD).required(true)
                .defaultValue("")
                .description("Колонка (поле) в таблице, которая используется в формуле для прогнозирования как временная ось."));
        
        
        return gssSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledGssSink(copyOf());
    }

    static class CompiledGssSink extends CompiledExternalSink {
        
        private final GssPrognoseSink gssSink;
        
        GssListUtils gssList;
        private ListFeed listFeed;
        
        protected CompiledGssSink(GssPrognoseSink gssSink) {
            super(gssSink);            
            this.gssSink = gssSink;            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            
            Event event = eventsByInputId.get(1);
                        
            if (event != null) {
                try {  
                    if(gssList == null){
                         gssList = new GssListUtils(
                                 DEFAULT_NAME, gssSink.getWorkBook(), gssSink.getSpreadSheet(), 
                                 gssSink.getUserEmail(), gssSink.getPassword()
                            );                         
                          setListFeed(gssList.loadSheet());                          
                    }
                    
                    String keyList      = gssSink.getOutFieldList();
                    String formulaList  = gssSink.getFormulaList().isEmpty() ? null : gssSink.getFormulaList();
                    Map<String, Object> map = event.getData();
 
logger.log(     Level.INFO, "event.getData(): ==> {0}", map);                    
                    
                    Integer index = (Integer) map.get(GssListUtils.REC_NUMBER_NAME);
                    
logger.log(     Level.INFO, "Integer index: ==> {0}", index);   

                    String newEntry;
                    if(keyList.isEmpty()){
                        newEntry = getGssListEntryFromEvent(map);
                    } else {
                        newEntry = getGssListEntryFromEvent(map, keyList);
                    }
                    
                    listFeed = getListFeed();
                    List<ListEntry> list = listFeed.getEntries();
                    if(index != null && list.size() > index){
                        ListEntry entry = gssList.updateEntryValues(list.get(index), newEntry);
                        
                        if (formulaList != null && !formulaList.isEmpty()) {
                            gssList.updateEntryFormulas(entry, newEntry, formulaList);
                        }
                        entry.update();
                    } else {
                        ListEntry entry = gssList.addNewEntryValues(newEntry);
                        gssList.getService().insert(gssList.getListFeedUrl(), entry);
                        
                        if (formulaList != null && !formulaList.isEmpty()){
                            gssList.updateEntryFormulas(entry, newEntry, formulaList);
                            entry.update(); 
                        }
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
                    entryValueString = entry.getValue().toString();
                    entryKeyString = entry.getKey().toString();
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
