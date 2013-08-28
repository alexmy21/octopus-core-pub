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
import com.google.gson.Gson;
import java.util.List;
import java.util.Set;
import org.lisapark.octopus.core.ModelBean;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.util.neo4j.Db4oNeo4jUtils;
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
public class Neo4jSinkContextSource extends ExternalSource {
    
    static final Logger LOG = LoggerFactory.getLogger(Neo4jSinkContextSource.class);
    
    private static final String DEFAULT_NAME        = "Neo4j Model Sink-Context";
    private static final String DEFAULT_DESCRIPTION = "Creates Model Context based on specified Sink processor.";

    private static final int URL_PARAMETER_ID           = 1;
    private static final int USER_NAME_PARAMETER_ID     = 2;
    private static final int PASSWORD_PARAMETER_ID      = 3;
//    private static final int PROC_TYPE_PARAMETER_ID     = 4;
    private static final int SINK_CLASS_PARAMETER_ID    = 5;

    private Neo4jSinkContextSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Neo4jSinkContextSource(UUID sourceId, Neo4jSinkContextSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Neo4jSinkContextSource(Neo4jSinkContextSource copyFromSource) {
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

    public String getSinkClass() {
        return getParameter(SINK_CLASS_PARAMETER_ID).getValueAsString();
    }

//    public String getProcType() {
//        return getParameter(PROC_TYPE_PARAMETER_ID).getValueAsString();
//    }

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public Neo4jSinkContextSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Neo4jSinkContextSource(sourceId, this);
    }

    @Override
    public Neo4jSinkContextSource copyOf() {
        return new Neo4jSinkContextSource(this);
    }

    public static Neo4jSinkContextSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Neo4jSinkContextSource neo4j = new Neo4jSinkContextSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        neo4j.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL")
                .defaultValue("http://localhost:7474/db/data/")
                .required(true));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:"));
        
//        db4o.addParameter(Parameter.stringParameterWithIdAndName(PROC_TYPE_PARAMETER_ID, "Proc Type:")
//                .defaultValue("")
//                .required(true));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(SINK_CLASS_PARAMETER_ID, "Sink Class Name:")
                .defaultValue("org.lisapark.octopus.core.sink.external.impl.ConsoleSink")
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
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, ModelBean.AUTHOR_EMAIL));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, ModelBean.DESCRIPTION));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, ModelBean.MODEL_JSON));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, ModelBean.MODEL_NAME));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private static class CompiledNeo4jQuerySource implements CompiledExternalSource {
        
        private final Neo4jSinkContextSource source;
        private volatile boolean running;
        private GraphDatabaseService graphDb = null; 
        private Db4oNeo4jUtils utils;

        public CompiledNeo4jQuerySource(Neo4jSinkContextSource source) {
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
                this.utils = new Db4oNeo4jUtils();
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
           
            Set<ModelBean> sinkList;
            sinkList = utils.getSinkRelatedModels(graphDb, source.getSinkClass());

            List<ModelBean> beanList = Lists.newArrayList();
            
            for(ModelBean item : sinkList){                
                beanList.add(item);
            }
            processProcList(beanList, runtime);
        }

        void processProcList(List<ModelBean> procList, ProcessingRuntime runtime) {
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

        Event createEventFromProcList(ModelBean bean, EventType eventType){
            Map<String, Object> attributeValues = Maps.newHashMap();
            
            attributeValues.put(ModelBean.MODEL_NAME, bean.getModelName());
            attributeValues.put(ModelBean.AUTHOR_EMAIL, bean.getAuthorEmail());
            attributeValues.put(ModelBean.MODEL_JSON, new Gson().toJson(bean, ModelBean.class));
            attributeValues.put(ModelBean.DESCRIPTION, bean.getDescription());
            
            LOG.info("Attribute MAP: " + attributeValues);

            return new Event(attributeValues);
        }
    }
}
