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
public class PrognoseToExcelSink extends AbstractNode implements ExternalSink {
    
     private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(PrognoseToExcelSink.class.getName());    
    
    private static final String DEFAULT_NAME        = "Вывод прогноза";
    private static final String DEFAULT_DESCRIPTION = "Вывод результатов прогнозирования в Excel";
    
    private static final int USER_EMAIL_PARAMETER_ID        = 1;
    private static final int PASSWORD_PARAMETER_ID          = 2;
    private static final int WORK_BOOK_PARAMETER_ID         = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID      = 4;
    private static final int ROW_NAME_PARAMETER_ID          = 5;
    private static final int SHOW_ROW_NAME_PARAMETER_ID     = 6;
    private static final int OUTPUT_FIELD_LIST_PARAMETER_ID = 7;
    private static final int FORMULA_LIST_PARAMETER_ID      = 8;
    
    private static final String USER_EMAIL          = "email Пользователя: ";
    private static final String PASSWORD            = "Пароль: ";
    private static final String WORKBOOK            = "Имя Эл. таблицы: ";
    private static final String SPREAD_SHEET        = "Имя таблицы: ";
    private static final String ROW_NAME            = "Имя поля с номером строки: ";
    private static final String SHOW_ROW_NAME       = "Выводить номер строки: ";
    private static final String OUTPUT_FIELD_LIST   = "Список выходных полей: ";
    private static final String FORMULA_LIST        = "Список формул: ";
    
    private static final String DEFAULT_INPUT       = "Входные данные";
    
    private static Map<Integer, String> paramMap    = new HashMap<Integer, String>();

    private Input<Event> input;

    private PrognoseToExcelSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private PrognoseToExcelSink(UUID id, PrognoseToExcelSink copyFromNode) {
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
    
    private PrognoseToExcelSink(PrognoseToExcelSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
  
//    @SuppressWarnings("unchecked")
//    public void setRowName(String rowName) throws ValidationException {
//        getParameter(ROW_NAME_PARAMETER_ID).setValue(rowName);
//    }
//
//    public String getRowName() {
//        return getParameter(ROW_NAME_PARAMETER_ID).getValueAsString();
//    }

    public Input<Event> getInput() {
        return input;
    }

    @SuppressWarnings("unchecked")
    public void setShowRowName(Boolean rowName) throws ValidationException {
        getParameter(SHOW_ROW_NAME_PARAMETER_ID).setValue(rowName);
    }

    public Boolean getShowRowName() {
        return (Boolean) getParameter(SHOW_ROW_NAME_PARAMETER_ID).getValue();
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
    public PrognoseToExcelSink newInstance() {
        return new PrognoseToExcelSink(UUID.randomUUID(), this);
    }

    @Override
    public PrognoseToExcelSink copyOf() {
        return new PrognoseToExcelSink(this);
    }

    public static PrognoseToExcelSink newTemplate() {
        UUID sinkId = UUID.randomUUID();

        PrognoseToExcelSink gssSink = new PrognoseToExcelSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, USER_EMAIL).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, WORKBOOK).required(true));
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, SPREAD_SHEET).required(true));
        
//        gssSink.addParameter(Parameter.stringParameterWithIdAndName(ROW_NAME_PARAMETER_ID, ROW_NAME).required(false)
//                .defaultValue(""));
//        gssSink.addParameter(Parameter.booleanParameterWithIdAndName(SHOW_ROW_NAME_PARAMETER_ID, SHOW_ROW_NAME).required(false)
//                .defaultValue(Boolean.TRUE));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(OUTPUT_FIELD_LIST_PARAMETER_ID, OUTPUT_FIELD_LIST).required(false)
                .defaultValue("")
                .description("Перечень разделенных запятыми полей, которые должны быть выведены в эектронную таблицу."));
        
        gssSink.addParameter(Parameter.stringParameterWithIdAndName(FORMULA_LIST_PARAMETER_ID, FORMULA_LIST).required(false)
                .defaultValue("")
                .description("Перечень разделенных запятыми полей, которые содержат формулы. Формат записи:"
                + " TOTAL1=PRICE1*QTY1,AVE=(TOTAL1+TOTAL2+TOTAL3)/3."
                + " Здесь TOTAL1, AVE, TOTAL2, TOTAL3, QTY1 - имена соответствующих колонок,"
                + " которые записаны в первой сроке электронной таблицы."));
        
        return gssSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledGssSink(copyOf());
    }

    static class CompiledGssSink extends CompiledExternalSink {
        
        private final PrognoseToExcelSink gssSink;
        
        GssListUtils gssList;
        private ListFeed listFeed;
        
        protected CompiledGssSink(PrognoseToExcelSink gssSink) {
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
            
            for(int i =0; i < keys.length; i++){
                keySet.add(keys[i]);
            }
            
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
