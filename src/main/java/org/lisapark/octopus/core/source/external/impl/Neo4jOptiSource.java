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

import choco.Options;
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
import org.lisapark.octopus.util.cpneo4j.CpNeo4jUtils;
import org.lisapark.octopus.util.cpneo4j.MachineResource;
import org.lisapark.octopus.util.cpneo4j.Product;
import org.lisapark.octopus.util.cpneo4j.Solution;
import org.lisapark.octopus.util.cpneo4j.TechnologyStep;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.openide.util.Exceptions;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class Neo4jOptiSource extends ExternalSource {

    private static final String DEFAULT_NAME            = "Optimization with Neo4j";
    private static final String DEFAULT_DESCRIPTION     = "Performs optimization using data from Neo4j"
            + " and Google Spreadsheet. Neo4j provides a structure of the optimization problem,"            
            + " Google Spreadsheet provides a source data.";
    
    private static final int NEO_URL                    = 1;
    private static final int NEO_USER_ID                = 2;
    private static final int NEO_PASSWORD               = 3;
    
    private static final int USER_EMAIL_PARAMETER_ID    = 5;
    private static final int PASSWORD_PARAMETER_ID      = 6;
    private static final int WORK_BOOK_PARAMETER_ID     = 7;
    private static final int SPREAD_SHEET_PARAMETER_ID  = 8;
    private static final int ROW_START_PARAMETER_ID     = 9;
    private static final int ROW_END_PARAMETER_ID       = 10;
    private static final int INDEX_PARAMETER_ID         = 11;
    
    private static final int NEO_SOLUTION_ID            = 12;
    private static final int OPTI_OPTION                = 13;
    private static final int OPTI_RESULT_FIELD_NAME     = 14;
    private static final int OPTI_TIME_OUT              = 15;
    private static final int OPTI_HIGH_COST             = 16;
    private static final int OPTI_HIGH_RESOURCES        = 17;
    private static final int OPTI_DOMAIN_OPTION         = 18;
    
    private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Neo4jOptiSource.class.getName());

    private Neo4jOptiSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Neo4jOptiSource(UUID sourceId, Neo4jOptiSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Neo4jOptiSource(Neo4jOptiSource copyFromSource) {
        super(copyFromSource);
    }

    private String getNeoUrl(){
        return getParameter(NEO_URL).getValueAsString();
    }
    
    private String getNeoUserId(){
        return getParameter(NEO_USER_ID).getValueAsString();
    }
    
    private String getNeoPassword(){
        return getParameter(NEO_PASSWORD).getValueAsString();
    }
  
    private String getNeoSolutionId(){
        return getParameter(NEO_SOLUTION_ID).getValueAsString();
    }
     
    private String getOptiOption(){
        return getParameter(OPTI_OPTION).getValueAsString();
    }
      
    private String getOptiResultFieldName(){
        return getParameter(OPTI_RESULT_FIELD_NAME).getValueAsString();
    }
       
    private String getOptiDomainOption(){
        return getParameter(OPTI_DOMAIN_OPTION).getValueAsString();
    }
        
    private Integer getOptiTimeOut(){
        return getParameter(OPTI_TIME_OUT).getValueAsInteger();
    }
         
    private Integer getOptiHighCost(){
        return getParameter(OPTI_HIGH_COST).getValueAsInteger();
    }
          
    private Integer getOptiHighResources(){
        return getParameter(OPTI_HIGH_RESOURCES).getValueAsInteger();
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

    public Integer getRowStart() {
        return getParameter(ROW_START_PARAMETER_ID).getValueAsInteger();
    }

    public Integer getRowEnd() {
        return getParameter(ROW_END_PARAMETER_ID).getValueAsInteger();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }
    
    
    public String getIndexFieldName() {
        return getParameter(INDEX_PARAMETER_ID).getValueAsString();
    }

    @Override
    public Neo4jOptiSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Neo4jOptiSource(sourceId, this);
    }

    @Override
    public Neo4jOptiSource copyOf() {
        return new Neo4jOptiSource(this);
    }

    /**
     * 
     * @return 
     */
    public static Neo4jOptiSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Neo4jOptiSource neoOptiSource = new Neo4jOptiSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(NEO_URL, "Neo4j URL: ")
                .defaultValue("http://localhost:7474/db/data")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(NEO_USER_ID, "Neo4j User ID: ")
                .defaultValue("")
                .required(false));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(NEO_PASSWORD, "Neo4j Password: ")
                .defaultValue("")
                .required(false));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(NEO_SOLUTION_ID, "Neo4j Solution ID: ")
                .defaultValue(CpNeo4jUtils.ROOT_SOLUTION)
                .required(true));        
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(OPTI_OPTION, "Optimization MAX/MIN: ")
                .defaultValue("MAX_UTILIZATION")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(OPTI_RESULT_FIELD_NAME, "Opti Result field: ")
                .defaultValue("production")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(OPTI_DOMAIN_OPTION, "Opti domain option: ")
                .defaultValue(Options.V_BOUND)
                .required(true));
        
        neoOptiSource.addParameter(Parameter.integerParameterWithIdAndName(OPTI_TIME_OUT, "Opti time-out: ")
                .defaultValue(15000)
                .required(true));
        
        neoOptiSource.addParameter(Parameter.integerParameterWithIdAndName(OPTI_HIGH_COST, "Opti high cost: ")
                .defaultValue(1000000)
                .required(true));
        
        neoOptiSource.addParameter(Parameter.integerParameterWithIdAndName(OPTI_HIGH_RESOURCES, "Opti high resources: ")
                .defaultValue(1000000)
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(USER_EMAIL_PARAMETER_ID, "User email: ")
                .defaultValue("demo1@lisa-park.com")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password: ")
                .defaultValue("isasdemo")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(WORK_BOOK_PARAMETER_ID, "Spreadsheet name: ")
                .defaultValue("CpNeo4jData")
                .required(true));
        
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(SPREAD_SHEET_PARAMETER_ID, "Sheet name: ")
                .defaultValue("TechnologySteps")
                .required(true));

        neoOptiSource.addParameter(Parameter.integerParameterWithIdAndName(ROW_START_PARAMETER_ID, "Start row: ")
                .defaultValue(0)
                .required(false));
        
        neoOptiSource.addParameter(Parameter.integerParameterWithIdAndName(ROW_END_PARAMETER_ID, "End row: ")
                .defaultValue(20)
                .required(false));
         
        neoOptiSource.addParameter(Parameter.stringParameterWithIdAndName(INDEX_PARAMETER_ID, "Index field name: ")
                .defaultValue("index")
                .description("This allows you to add index to the range of selected rows. "
                + "You will need this index, if you plan to make any statistical calculations that use time window "
                + "like: Moving Average, Correlation, Regression and etc.")
                .required(true));

        neoOptiSource.setOutput(Output.outputWithId(1).setName("Output data"));
        
        neoOptiSource.addAttributeList();

        return neoOptiSource;
    }

    /**
     * 
     */
    private void addAttributeList() {
        try {
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, "id"));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, TechnologyStep.DESCRIPTION));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, Product.PRODUCT_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.MACHINE_RESOURCE_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, TechnologyStep.PRODUCTION));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, TechnologyStep.STEPS_REQUIRED));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, TechnologyStep.TECHNOLOGY_STEP_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, TechnologyStep.TECHNOLOGY_STEP_JSON));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, TechnologyStep.TECHNOLOGY_STEP_NAME));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, TechnologyStep.RESOURCE_REQUIRED));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, TechnologyStep.STEP_STATUS));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, TechnologyStep.TECH_STEP_NUMBER));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }
    
    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();
        return new CompiledNeoOptiSource(this.copyOf());
    }

    private static class CompiledNeoOptiSource implements CompiledExternalSource {

        private final Neo4jOptiSource source;
        private volatile boolean running;

        public CompiledNeoOptiSource(Neo4jOptiSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }
            
            // Connect to Neo4j and perform optimization
            //==================================================================
            CpNeo4jUtils utils = new CpNeo4jUtils();

            if (utils.getGraphDbService() == null) {
                if (utils.checkDatabaseIsRunning(source.getNeoUrl()) == 200) {
                    utils.setGraphDbService(utils.newServerInstance(
                            source.getNeoUrl(),
                            source.getNeoUserId(),
                            source.getNeoPassword()));
                } else {
                    logger.log(Level.SEVERE, "Neo4j server is not available");
                    return;
                }
            }

            GraphDatabaseService _graphDb = utils.getGraphDbService();

            IndexManager index = _graphDb.index();
            Index<Node> solutions = index.forNodes(CpNeo4jUtils.SOLUTIONS);

            IndexHits<Node> hits = solutions.get(CpNeo4jUtils.SOLUTION_NAME, source.getNeoSolutionId());

            if (hits.size() == 0) {
                return;
            }

            Node solutionNode = hits.getSingle();

            Solution solution = new Solution(
                    source.getOptiResultFieldName(), 
                    source.getOptiHighCost(), 
                    source.getOptiHighResources(), 
                    source.getOptiDomainOption());

            Map<String, Integer> optiMap = solution.solve(solutionNode, source.getOptiTimeOut(), source.getOptiOption());
            
            // Extract data from Google Spreadsheet and merge Optimization
            // Results and data from spreadsheet.
            //==================================================================
            GssSourceRangeUtils gss = new GssSourceRangeUtils(
                    source.getName(),
                    source.getWorkBook(),
                    source.getSpredSheet(),
                    source.getUseremail(),
                    source.getPassword(), 
                    source.getRowStart(), 
                    source.getRowEnd());
            try {
                List<Map<String, Object>> list = gss.loadSheet();
                if (list != null) {
                    processCellRange(list, optiMap, runtime);
                }
            } catch (ServiceException ex) {
                Exceptions.printStackTrace(ex);
            } catch (IOException ex) {
                Exceptions.printStackTrace(ex);
            }
        }

        void processCellRange(List<Map<String, Object>> data, Map<String, Integer> optiMap, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            String indexName = source.getIndexFieldName();
            for (Map<String, Object> item : data) {
                if (thread.isInterrupted()) {
                    break;
                }
                // Update production Field with optiResult
                String techStepId = (String) item.get(TechnologyStep.TECHNOLOGY_STEP_ID.toLowerCase());
                Integer production = optiMap.get(techStepId);
                item.put(TechnologyStep.PRODUCTION, production);
                
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

        class GssSourceRangeUtils {

            // Parameter List
            //==========================================================================
            private final String spreadSheetName;
            private final String workSheetName;
            private final int start;
            private final int end;
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
            GssSourceRangeUtils(String serviceId, String spreadSheet,
                    String workSheet, String userEmail, String password,
                    int start, int end) {
                this.spreadSheetName = spreadSheet;
                this.workSheetName = workSheet;
                this.start = start;
                this.end = end;

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
                listFeedUrl = worksheet.getListFeedUrl();

                ListFeed listFeed = service.getFeed(listFeedUrl, ListFeed.class);

                return getEntriyRangeFromListFeed(listFeed, start, end);
            }

            /**
             * Extracts all entries from ListFeed and converts each entry to the
             * map. Map<String, Object>.
             *
             * @param feed
             * @return List of entry maps.
             */
            List<Map<String, Object>> getEntriyRangeFromListFeed(ListFeed feed, int start, int end) {

                List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

                List<ListEntry> listEntries = feed.getEntries();
                if (start > end || end == 0 || end > listEntries.size()) {
                    // Read from start to the end of listEntries
                    end = listEntries.size();
                }

                for (int i = start; i < end; i++) {
                    Map<String, Object> map = new HashMap<String, Object>();
                    ListEntry entry = listEntries.get(i);
                    for (String tag : entry.getCustomElements().getTags()) {
                        map.put(tag, entry.getCustomElements().getValue(tag));
                    }
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
                return index == worksheets.size() ? -1 : index;
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
