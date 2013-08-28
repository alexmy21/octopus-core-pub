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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.ModelBean;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.external.CompiledExternalSink;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.util.neo4j.Db4oNeo4jUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class ModelJson2Neo4jSrvSink extends AbstractNode implements ExternalSink {
    
    static final Logger LOG = LoggerFactory.getLogger(ModelJson2Neo4jSrvSink.class);
    
    private static final String DEFAULT_NAME = "Model Json to Neo4j Server";
    private static final String DEFAULT_DESCRIPTION = "Outputs Model json to Rest Server Neo4j database.";
    private static final String DEFAULT_INPUT = "Input";    
    
    private static final int NEO4J_URL_PARAMETER_ID         = 1;
    private static final String NEO4J_URL                   = "URL:";
    private static final String NEO4J_URL_DESCRIPTION       = "Neo4j Server URL.";
    
    private static final int USER_ID_PARAMETER_ID           = 2;
    private static final String USER_ID                     = "User ID:";
    private static final String USER_ID_DESCRIPTION         = "User ID.";
    
    private static final int PASSWORD_PARAMETER_ID           = 3;
    private static final String PASSWORD                     = "Password:";
    private static final String PASSWORD_DESCRIPTION         = "Password.";
    
    private static final int ATTRIBUTE_LIST_PARAMETER_ID    = 4;
    private static final String ATTRIBUTE_LIST              = "Show Attributes";
    private static final String ATTRIBUTE_LIST_DESCRIPTION  = 
            "List comma separated attribute names that you would like to show on Console. Empty - will show all attributes.";
    
    private Input<Event> input;

    private ModelJson2Neo4jSrvSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ModelJson2Neo4jSrvSink(UUID id, ModelJson2Neo4jSrvSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    private ModelJson2Neo4jSrvSink(ModelJson2Neo4jSrvSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }
    
    public String getNeo4jUrl() {
        return getParameter(NEO4J_URL_PARAMETER_ID).getValueAsString();
    }
    
    public String getUserId() {
        return getParameter(USER_ID_PARAMETER_ID).getValueAsString();
    }
    
    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }
    
    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
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
    public ModelJson2Neo4jSrvSink newInstance() {
        return new ModelJson2Neo4jSrvSink(UUID.randomUUID(), this);
    }

    @Override
    public ModelJson2Neo4jSrvSink copyOf() {
        return new ModelJson2Neo4jSrvSink(this);
    }

    public static ModelJson2Neo4jSrvSink newTemplate() {
        UUID sinkId = UUID.randomUUID();
        
        ModelJson2Neo4jSrvSink neo4jSink = new ModelJson2Neo4jSrvSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        
        neo4jSink.addParameter(
                Parameter.stringParameterWithIdAndName(NEO4J_URL_PARAMETER_ID, NEO4J_URL)
                .defaultValue("http://localhost:7474/db/data/")
                .description(NEO4J_URL_DESCRIPTION)
                );
        neo4jSink.addParameter(
                Parameter.stringParameterWithIdAndName(USER_ID_PARAMETER_ID, USER_ID)
                .defaultValue("")
                .description(USER_ID_DESCRIPTION)
                );
        neo4jSink.addParameter(
                Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD)
                .defaultValue("")
                .description(PASSWORD_DESCRIPTION)
                );
        neo4jSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                .defaultValue("")
                .description(ATTRIBUTE_LIST_DESCRIPTION)
                );
        
        return neo4jSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledModel2Neo4jSink(copyOf());
    }

    static class CompiledModel2Neo4jSink extends CompiledExternalSink {
        
        private ModelJson2Neo4jSrvSink sink;
        private GraphDatabaseService graphDb = null; 
        Db4oNeo4jUtils utils;
                
        protected CompiledModel2Neo4jSink(ModelJson2Neo4jSrvSink processor) {
            super(processor);
            this.sink = processor;
            
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
            Event event = eventsByInputId.get(1);
            
            if (utils == null) {
                this.utils = new Db4oNeo4jUtils();
            }

            if (utils.getGraphDbService() == null) {
                utils.setGraphDbService(utils.newServerInstance(sink.getNeo4jUrl()));
            }
  
            graphDb = utils.getGraphDbService();
            
            LOG.info("Event: " + event);
            
            String modelStr = (String) event.getData().get(ModelBean.MODEL_JSON);
            
            LOG.info("Model JSON: " + modelStr);            
            
            if (graphDb != null && !modelStr.trim().isEmpty()) {               
                utils.addModelContext2Cluster(modelStr, graphDb);                
                ctx.getStandardOut().println(modelStr);
            } else {
                ctx.getStandardOut().println("event is null or empty");
            }
        } 
        
        @Override
        protected void finalize() throws Throwable{
            utils.getGraphDbService().shutdown();            
            super.finalize();            
        }
    }
}
