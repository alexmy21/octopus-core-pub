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
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import org.lisapark.octopus.core.event.Attribute;
import static org.lisapark.octopus.core.source.external.impl.Neo4jMachineResourceSource.LOG;
import org.lisapark.octopus.util.cpneo4j.CpNeo4jUtils;
import org.lisapark.octopus.util.cpneo4j.MachineResource;
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
public class Neo4jMachineResourceSource extends ExternalSource {
    
    static final Logger LOG = LoggerFactory.getLogger(Neo4jMachineResourceSource.class);
    
    private static final String DEFAULT_NAME        = "Neo4j Machines";
    private static final String DEFAULT_DESCRIPTION = "Extract machines from Neo4j database.";

    private static final int URL_PARAMETER_ID           = 1;
    private static final int USER_NAME_PARAMETER_ID     = 2;
    private static final int PASSWORD_PARAMETER_ID      = 3;

    private Neo4jMachineResourceSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Neo4jMachineResourceSource(UUID sourceId, Neo4jMachineResourceSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Neo4jMachineResourceSource(Neo4jMachineResourceSource copyFromSource) {
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

    public EventType getEventType() {
        return getOutput().getEventType();
    }

    @Override
    public Neo4jMachineResourceSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Neo4jMachineResourceSource(sourceId, this);
    }

    @Override
    public Neo4jMachineResourceSource copyOf() {
        return new Neo4jMachineResourceSource(this);
    }

    public static Neo4jMachineResourceSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Neo4jMachineResourceSource neo4j = new Neo4jMachineResourceSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        neo4j.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, "URL")
                .defaultValue("http://localhost:7474/db/data/")
                .required(true));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, "User name"));
        neo4j.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, "Password:"));
        
        neo4j.setOutput(Output.outputWithId(1).setName("JSON"));
        
        neo4j.addAttributeList();

        return neo4j;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        validate();
        return new Neo4jMachineResourceSource.CompiledMachinesFromNeo4jSource(this.copyOf());
    }
    
    private void addAttributeList() {
        try {
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.DESCRIPTION));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, MachineResource.LOW_BOUND));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, MachineResource.UPPER_BOUND));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, MachineResource.RESOURCE_TOTAL));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.MACHINE_RESOURCE_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.MACHINE_JSON));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.MACHINE_NAME));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, MachineResource.MEASURE_UNIT));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, MachineResource.RESOURCE_UNIT_COST));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, MachineResource.RESOURCE_USED));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private static class CompiledMachinesFromNeo4jSource implements CompiledExternalSource {
        
        private final Neo4jMachineResourceSource source;
        private volatile boolean running;
        private GraphDatabaseService graphDb = null; 
        private CpNeo4jUtils utils;

        public CompiledMachinesFromNeo4jSource(Neo4jMachineResourceSource source) {
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
                if (utils.checkDatabaseIsRunning(source.getUrl()) == 200) {
                    utils.setGraphDbService(utils.newServerInstance(source.getUrl()));
                } else {
                    LOG.error("Neo4j server is not available.");
                    return;
                }
            }

            graphDb = utils.getGraphDbService();

            Set<Map<String, Object>> machineSet;

            machineSet = utils.getAllMachines(graphDb);

            List<Map<String, Object>> machineList = Lists.newArrayList();
            
            for(Map<String, Object> item : machineSet){                
                machineList.add(item);
            }
            processMachineResourceList(machineList, runtime);
        }

        void processMachineResourceList(List<Map<String, Object>> machineList, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();

            int i = 0;
            while (!thread.isInterrupted() && running && i < machineList.size()) {
                Map<String, Object> attributeValues = Maps.newHashMap(machineList.get(i));
                Event newEvent;
                newEvent = new Event(attributeValues);
                runtime.sendEventFromSource(newEvent, source);
                i++;
            }
        }

        @Override
        public void stopProcessingEvents() {
            this.running = false;
        }
    }
}
