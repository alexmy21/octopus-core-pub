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
import com.google.gson.Gson;
import java.util.List;
import java.util.Set;
import org.lisapark.octopus.core.ModelBean;
import org.lisapark.octopus.core.event.Attribute;
import static org.lisapark.octopus.core.source.external.impl.Neo4jTechnologyStepsSource.LOG;
import org.lisapark.octopus.util.cpneo4j.CpNeo4jUtils;
import org.lisapark.octopus.util.cpneo4j.Product;
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
public class Neo4jProductSource extends ExternalSource {
    
    static final Logger LOG = LoggerFactory.getLogger(Neo4jProductSource.class);
    
    private static final String DEFAULT_NAME        = "Neo4j Products";
    private static final String DEFAULT_DESCRIPTION = "Creates Model Context based on specified Sink processor.";

    private static final int URL_PARAMETER_ID           = 1;
    private static final int USER_NAME_PARAMETER_ID     = 2;
    private static final int PASSWORD_PARAMETER_ID      = 3;

    private Neo4jProductSource(UUID sourceId, String name, String description) {
        super(sourceId, name, description);
    }

    private Neo4jProductSource(UUID sourceId, Neo4jProductSource copyFromSource) {
        super(sourceId, copyFromSource);
    }

    private Neo4jProductSource(Neo4jProductSource copyFromSource) {
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
    public Neo4jProductSource newInstance() {
        UUID sourceId = UUID.randomUUID();
        return new Neo4jProductSource(sourceId, this);
    }

    @Override
    public Neo4jProductSource copyOf() {
        return new Neo4jProductSource(this);
    }

    public static Neo4jProductSource newTemplate() {
        UUID sourceId = UUID.randomUUID();
        Neo4jProductSource neo4j = new Neo4jProductSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

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
        return new CompiledNeo4jQuerySource(this.copyOf());
    }
    
    private void addAttributeList() {
        try {
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, Product.DESCRIPTION));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, Product.PRODUCTION_LOW));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, Product.PRODUCTION_UPPER));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, Product.PRODUCTION_VALUE));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, Product.PRODUCT_ID));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, Product.PRODUCT_JSON));
            this.getEventType().addAttribute(Attribute.newAttribute(String.class, Product.PRODUCT_NAME));
            this.getEventType().addAttribute(Attribute.newAttribute(Integer.class, Product.PRODUCT_PRICE));
        } catch (ValidationException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private static class CompiledNeo4jQuerySource implements CompiledExternalSource {
        
        private final Neo4jProductSource source;
        private volatile boolean running;
        private GraphDatabaseService graphDb = null; 
        private CpNeo4jUtils utils;

        public CompiledNeo4jQuerySource(Neo4jProductSource source) {
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

            Set<Map<String, Object>> productSet;

            productSet = utils.getAllProducts(graphDb);

            List<Map<String, Object>> productList = Lists.newArrayList();
            
            for(Map<String, Object> item : productSet){                
                productList.add(item);
            }
            processProductList(productList, runtime);
        }

        void processProductList(List<Map<String, Object>> productList, ProcessingRuntime runtime) {
            Thread thread = Thread.currentThread();

            int i = 0;
            while (!thread.isInterrupted() && running && i < productList.size()) {
                Map<String, Object> attributeValues = Maps.newHashMap(productList.get(i));
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
