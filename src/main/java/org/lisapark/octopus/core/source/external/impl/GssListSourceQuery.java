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
package org.lisapark.octopus.core.source.external.impl;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Maps;
import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;
import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.util.Booleans;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class GssListSourceQuery extends ExternalSource {

    private static final String DEFAULT_NAME = "Google Spreadsheet Query";
    private static final String DEFAULT_DESCRIPTION = "Provides access to Google Spreadsheet."
            + " Can be used to retrieve rows, using provided query. Query is WHERE clause of the regular"
            + " SQL statement, for example: age<60 and city='Philadelphia'.";
    private static final int USER_EMAIL_PARAMETER_ID    = 1;
    private static final int PASSWORD_PARAMETER_ID      = 2;
    private static final int WORK_BOOK_PARAMETER_ID     = 3;
    private static final int SPREAD_SHEET_PARAMETER_ID  = 4;
    private static final int QUERY_PARAMETER_ID         = 5;
    private static final int INDEX_PARAMETER_ID         = 6;
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(GssListSourceQuery.class.getName());    

    private GssListSourceQuery(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private GssListSourceQuery(UUID sourceId, GssListSourceQuery copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private GssListSourceQuery(GssListSourceQuery copyFromSource) {
        super(copyFromSource);
    }

    public String getWorkBook() {
        return getParameter(WORK_BOOK_PARAMETER_ID).getValueAsString();
    }

    public String getUseremail() {
        return getParameter(USER_EMAIL_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getSpredSheet() {
        return getParameter(SPREAD_SHEET_PARAMETER_ID).getValueAsString();
    }

    public String getQuery() {
        return getParameter(QUERY_PARAMETER_ID).getValueAsString();
    }

    public String getIndexFieldName() {
        return getParameter(INDEX_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public GssListSourceQuery newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new GssListSourceQuery(sourceId, this);
    }

    @Override
    public GssListSourceQuery copyOf() {
        return new GssListSourceQuery(this);
    }

    public static GssListSourceQuery newTemplate() {
        UUID sourceId = UUID.randomUUID();
        GssListSourceQuery gssSource = new GssListSourceQuery(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        gssSource.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, "User email: ")
                .defaultValue("")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password: ")
                .defaultValue("")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, "Spreadsheet name: ")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, "Sheet name: ")
                .defaultValue("")
                .required(true));
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(QUERY_PARAMETER_ID, "Query: ")
                .defaultValue("")
                .description("Query is the same as it would be in the WHERE clause of "
                + "standard SQL statement. Note that column/field name is in low case.")
                .required(false));
 
        gssSource.addParameter(Parameter.stringParameterWithIdAndName(INDEX_PARAMETER_ID, "Index field name: ")
                .defaultValue("index")
                .description("This allows you to add index to the range of selected rows. "
                + "You will need this index, if you plan to make any statistical calculations that use time window "
                + "like: Moving Average, Correlation, Regression and etc. "
                + "Do not forget to add index field name to the Event Attribute List in the Output property.")
                .required(true));

        gssSource.setOutput(Output.outputWithId(1).setName("Output data"));

        return gssSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();

        return new CompiledGssListSource(this.copyOf());
    }

    private static class CompiledGssListSource implements CompiledExternalSource {

        private final GssListSourceQuery source;
        private volatile boolean running;

        public CompiledGssListSource(GssListSourceQuery source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }

            GssSourceQueryUtils gss = new GssSourceQueryUtils(
                    source.getName(),
                    source.getWorkBook(),
                    source.getSpredSheet(),
                    source.getUseremail(),
                    source.getPassword(),
                    source.getQuery());
            try {
                List<Map<String, Object>> list = gss.loadSheet();
                processCellRange(list, runtime);

            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        void processCellRange(List<Map<String, Object>> data, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            String indexName = source.getIndexFieldName();
            for (Map<String, Object> item : data) {
                if (thread.isInterrupted()) {
                    break;
                }
                // Add index to the list of data maps
                item.put(indexName, i);
                i++;
                
                Event newEvent = createEventFromCellRange(item, eventType);
                logger.log(Level.INFO, "processCellRange: ==> {0}", newEvent);
                runtime.sendEventFromSource(newEvent, source);
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromCellRange(Map<String, Object> cellRange, EventType eventType) {
            Map<String, Object> attributeValues = Maps.newHashMap();
            
            for (Attribute attribute : eventType.getAttributes()) {
                Class type = attribute.getType();
                String attributeName = attribute.getName().trim();
                Object objValue = cellRange.get(attributeName.toLowerCase().replace("_", ""));

                if (type == String.class) {
                    String value = "";
                    if (objValue != null) {
                        value = (String) objValue;
                    }
                    attributeValues.put(attributeName, value);

                } else if (type == Integer.class) {
                    int value = 0;
                    if (objValue != null && !objValue.toString().trim().isEmpty()) {
                        value = Integer.parseInt(objValue.toString());
                    }
                    attributeValues.put(attributeName, value);

                } else if (type == Short.class) {
                    Short value = 0;
                    if (objValue != null && !objValue.toString().trim().isEmpty()) {
                        value = Short.parseShort(objValue.toString());
                    }
                    
                    attributeValues.put(attributeName, value);

                } else if (type == Long.class) {
                    Long value = 0L;
                    if (objValue != null && !objValue.toString().trim().isEmpty()) {
                        value = Long.parseLong((String) objValue);
                    }

                    attributeValues.put(attributeName, value);

                } else if (type == Double.class) {
                    Double value = 0D;
                    if (objValue != null && !objValue.toString().trim().isEmpty()) {
                        value = Double.parseDouble(objValue.toString());
                    }

                    attributeValues.put(attributeName, value);

                } else if (type == Float.class) {
                    Float value = 0F;
                    if (objValue != null && !objValue.toString().trim().isEmpty()) {
                        value = Float.parseFloat(objValue.toString());
                    }

                    attributeValues.put(attributeName, value);

                } else if (type == Boolean.class) {
                    String value = (String) cellRange.get(attributeName);
                    attributeValues.put(attributeName, Booleans.parseBoolean(value));
                } else {
                    throw new IllegalArgumentException(String.format("Unknown attribute type %s", type));
                }
            }
            
            return new Event(attributeValues);
        }

        private int convert2Int(Object obj) throws NumberFormatException {
            int recNum;
            if (obj instanceof Integer) {
                recNum = (Integer) obj;
            } else if (obj instanceof Double) {
                recNum = (int) Math.floor((Double) obj);
            } else if (obj instanceof Float) {
                recNum = (int) Math.floor((Float) obj);
            } else {
                recNum = Integer.parseInt(obj.toString());
            }
            return recNum;
        }

        class GssSourceQueryUtils {

            // Parameter List
            //==========================================================================
            private final String spreadSheetName;
            private final String workSheetName;
            private final String query;
            // 
            private final SpreadsheetService service;
            private final FeedURLFactory factory;
            private URL listFeedUrl;

            /**
             *
             * @param serviceId
             * @param spreadSheet
             * @param workSheet
             * @param userEmail
             * @param password
             */
            GssSourceQueryUtils(String serviceId, String spreadSheet,
                    String workSheet, String userEmail, String password, String query) {
                this.spreadSheetName = spreadSheet;
                this.workSheetName = workSheet;
                this.query = query;

                this.service = new SpreadsheetService(serviceId);
                try {
                    service.setUserCredentials(userEmail, password);
                } catch (AuthenticationException ex) {
                    Exceptions.printStackTrace(ex);
                }

                this.factory = FeedURLFactory.getDefault();
            }

            /**
             *
             * @return @throws ServiceException
             * @throws IOException
             */
            List<Map<String, Object>> loadSheet() throws ServiceException, IOException {
                // Get the spreadsheet to load
                SpreadsheetFeed feed = service.getFeed(factory.getSpreadsheetsFeedUrl(),
                        SpreadsheetFeed.class);
                List<SpreadsheetEntry> spreadsheets = feed.getEntries();
                int spreadsheetIndex = getSpreadsheetIndex(spreadsheets, spreadSheetName);
                SpreadsheetEntry spreadsheet = null;
                if(spreadsheetIndex == -1){
                   throw new IllegalArgumentException(String.format("Unknown Spreadsheet name %s", spreadSheetName));
                } else {
                    spreadsheet = feed.getEntries().get(spreadsheetIndex);
                }

                // Get the worksheet to load       
                List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
                int worksheetIndex = getWorksheetIndex(worksheets, workSheetName);
                WorksheetEntry worksheet = null;
                if(worksheetIndex == -1){
                    throw new IllegalArgumentException(String.format("Unknown Worksheet name %s", spreadSheetName)); 
                } else {
                    worksheet = (WorksheetEntry) worksheets.get(worksheetIndex);
                }

                ListFeed listFeed;
                if (query == null || query.isEmpty()) {
                    listFeedUrl = worksheet.getListFeedUrl();
                } else {
                    try {
                        listFeedUrl = new URI(null, null, worksheet.getListFeedUrl().toString(), "sq=" + query.toLowerCase(), null).toURL();
                    } catch (URISyntaxException ex) {
                        Exceptions.printStackTrace(ex);
                    }
                }

                listFeed = service.getFeed(listFeedUrl, ListFeed.class);

                return getEntryQueryFromListFeed(listFeed);
            }

            /**
             * Extracts all entries from ListFeed and converts each entry to the
             * map. Map<String, Object>.
             *
             * @param feed
             * @return List of entry maps.
             */
            List<Map<String, Object>> getEntryQueryFromListFeed(ListFeed feed) {

                List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

                for (ListEntry entry : feed.getEntries()) {
                    Map<String, Object> map = new HashMap<String, Object>();

                    for (String tag : entry.getCustomElements().getTags()) {
                        map.put(tag, entry.getCustomElements().getValue(tag));
                    }
//            map.put(REC_NUMBER_NAME, new Integer(list.size()));
                    list.add(map);
                }
                return list;
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
                for (WorksheetEntry item : worksheets) {
                    if (item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) {
                        break;
                    }
                    index++;
                }
                return worksheets.size() == index ? -1 : index;
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
                for (SpreadsheetEntry item : spreadsheets) {
                    if (item.getTitle().getPlainText().trim().equalsIgnoreCase(itemNameTrimmed)) {
                        break;
                    }
                    index++;
                }
                return spreadsheets.size() == index ? -1 : index;
            }
        }
    }
}