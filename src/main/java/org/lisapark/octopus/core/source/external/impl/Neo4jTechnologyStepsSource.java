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

import com.google.common.collect.Maps;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ProcessingException;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.event.EventType;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;

import java.sql.ResultSet;
import java.util.Map;
import java.util.UUID;

import org.lisapark.octopus.core.source.external.CompiledExternalSource;
import org.lisapark.octopus.core.source.external.ExternalSource;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.util.cpneo4j.CpNeo4jUtils;
import org.lisapark.octopus.util.cpneo4j.MachineResource;
import org.lisapark.octopus.util.cpneo4j.Product;
import org.lisapark.octopus.util.cpneo4j.TechnologyStep;
import org.neo4j.graphdb.GraphDatabaseService;
import org.openide.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an {@link ExternalSource} that is used to access object databases. It can be configured with
 * a db4o Url, username, password, and a query to execute.
 * <p/>
 * Currently, the source uses the {@link org.lisapark.octopus.core.Output#getEventType()} to get the names of the
 * columns and types of the columns, but it will probably be changed in the future to support a mapper that takes
 * a {@link ResultSet} and produces an {@link Event}.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @author alex mylnikov(alexmy@lisa-park.com)
 */
@Persistable
public class Neo4jTechnologyStepsSource extends ExternalSource {
    
    static final Logger LOG = LoggerFactory.getLogger(Neo4jTechnologyStepsSource.class);
    // purpose 
    private static final String DEFAULT_NAME        = "Neo4j Technology Steps";
    private static final String DEFAULT_DESCRIPTION = "Extracts Technology Steps according to specified Product or"
            + " Machine Id. The main purpose of this Source Processor is to provide Model with events that have machine/product id and"
            + " corresponding technology steps as attributes for building relations between selected items."
            + " Event includes following attributes: QUERT_TYPE; TYPE_ID; JSON - string that represents technology step.";

    private static final int URL_PARAMETER_ID           = 1;
    private static final int USER_NAME_PARAMETER_ID     = 2;
    private static final int PASSWORD_PARAMETER_ID      = 3;
    private static final int QUERY_TYPE_PARAMETER_ID    = 4;
    private static final int TYPE_ID_PARAMETER_ID       = 5;    

    private Neo4jTechnologyStepsSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Neo4jTechnologyStepsSource(UUID sourceId, Neo4jTechnologyStepsSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Neo4jTechnologyStepsSource(Neo4jTechnologyStepsSource copyFromSource) {
        super(copyFromSource);
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }

    public String getUsername() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getTypeId() {
        return getParameter(TYPE_ID_PARAMETER_ID).getValueAsString();
    }

    public String getQueryType() {
        return getParameter(QUERY_TYPE_PARAMETER_ID).getValueAsString();
    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public Neo4jTechnologyStepsSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Neo4jTechnologyStepsSource(sourceId, this);
    }

    @Override
    public Neo4jTechnologyStepsSource copyOf() {
        return new Neo4jTechnologyStepsSource(this);
    }

    public static Neo4jTechnologyStepsSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Neo4jTechnologyStepsSource neo4j = new Neo4jTechnologyStepsSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        neo4j.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL")
                .defaultValue("http://localhost:7474/db/data/")
                .required(true));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:"));
        
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(QUERY_TYPE_PARAMETER_ID, "Query Type:")
                .defaultValue("PRODUCT")
                .description("Query type defined search criteria: PRODUCT, MACHINE. Use PRODUCT"
                + " to select all tech steps for specific Product; MACHINE - tech steps for specific machines.")
                .required(true));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(TYPE_ID_PARAMETER_ID, "Machine Id:")
                .defaultValue("")
                .description("ID value for selected type. Empty to select all relevant technology steps.")
                .required(false));

        neo4j.setOutput(Output.outputWithId(1).setName("JSON"));
        
        neo4j.addAttributeList();

        return neo4j;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();
        return new CompiledNeo4jQuerySource(this.copyOf());
    }
    
    private void addAttributeList() {
        try {
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, CpNeo4jUtils.QUERY_TYPE));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, CpNeo4jUtils.TYPE_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, CpNeo4jUtils.JSON));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private static class CompiledNeo4jQuerySource implements CompiledExternalSource {
        
        private final Neo4jTechnologyStepsSource source;
        private volatile boolean running;
        private GraphDatabaseService graphDb = null; 
        private CpNeo4jUtils utils;

        public CompiledNeo4jQuerySource(Neo4jTechnologyStepsSource source) {
            this.source = source;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {
            // this needs to be atomic, both the check and set
            synchronized (this) {
                checkState(!running, "Source is already processing events. Cannot call processEvents again");
                running = true;
            }             
            
            if (utils == null) {
                this.utils = new CpNeo4jUtils();
            }

            if (utils.getGraphDbService() == null) {
                if(utils.checkDatabaseIsRunning(source.getUrl()) == 200){
                    utils.setGraphDbService(utils.newServerInstance(source.getUrl()));
                } else {
                    LOG.error("Neo4j server is not available.");
                    return;
                }
            }
  
            graphDb = utils.getGraphDbService();
           
            Set<Map<String, String>> sinkList = Sets.newHashSet();
            
            if(Product.PRODUCT_ID.equalsIgnoreCase(source.getQueryType())){
                sinkList = utils.getProductTechSteps(graphDb, source.getTypeId());
            } else if(MachineResource.MACHINE_RESOURCE_ID.equalsIgnoreCase(source.getQueryType())){ 
                sinkList = utils.getMachineTechSteps(graphDb, source.getTypeId());                
            } else {
                sinkList = utils.getTechSteps(graphDb, source.getTypeId(), TechnologyStep.TECHNOLOGY_STEP_ID);
            }

            List<Map<String, String>> beanList = Lists.newArrayList();
            
            for(Map<String, String> item : sinkList){                
                beanList.add(item);
            }
            processProcList(beanList, runtime);
        }

        void processProcList(List<Map<String, String>> procList, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();
            EventType eventType = source.getEventType();

            int i = 0;
            while (!thread.isInterrupted() && running && i < procList.size()) {
                Event newEvent = createEventFromProcList(procList.get(i), eventType);
                runtime.sendEventFromSource(newEvent, source);
                i++;
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }

        Event createEventFromProcList(Map<String, String> bean, EventType eventType){
            Map<String, Object> attributeValues = Maps.newHashMap();
            
            attributeValues.put(CpNeo4jUtils.QUERY_TYPE, bean.get(CpNeo4jUtils.QUERY_TYPE));
            attributeValues.put(CpNeo4jUtils.TYPE_ID, bean.get(CpNeo4jUtils.TYPE_ID));
            attributeValues.put(CpNeo4jUtils.JSON, bean.get(CpNeo4jUtils.JSON));
            
            LOG.info("Attribute MAP: " + attributeValues);

            return new Event(attributeValues);
        }
    }
}
