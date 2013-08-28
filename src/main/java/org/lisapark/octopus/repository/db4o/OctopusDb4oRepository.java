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
package org.lisapark.octopus.repository.db4o;

import com.db4o.ObjectContainer;
import com.db4o.ObjectServer;
import com.db4o.ObjectSet;
import com.db4o.config.ConfigScope;
import com.db4o.cs.Db4oClientServer;
import com.db4o.cs.config.ServerConfiguration;
import com.db4o.ext.Db4oException;
import com.db4o.ext.Db4oIOException;
import com.db4o.query.Predicate;
import com.db4o.ta.DeactivatingRollbackStrategy;
import com.db4o.ta.TransparentActivationSupport;
import com.db4o.ta.TransparentPersistenceSupport;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.Source;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.repository.AbstractOctopusRepository;
import org.lisapark.octopus.repository.OctopusRepository;
import org.lisapark.octopus.repository.RepositoryException;
import org.openide.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class OctopusDb4oRepository extends AbstractOctopusRepository implements OctopusRepository {
    
private static final Logger LOG = LoggerFactory.getLogger(OctopusDb4oRepository.class);

    private static ObjectServer server = null;

    // Constructors
    //==========================================================================
    
    public OctopusDb4oRepository() {
              
    }
    
    public OctopusDb4oRepository(ObjectServer server) {
        OctopusDb4oRepository.server = server;      
    }
    
    public OctopusDb4oRepository(String fileName) {
        init(fileName, 0, "", "");
    }
   
    public OctopusDb4oRepository(String fileName, int port) {
        init(fileName, port, "", "");        
    }
    
    public OctopusDb4oRepository(String fileName, int port, String uid, String psw) {
        init(fileName, port, uid, psw);        
    }
    
    public static ObjectServer getServer() {
        return server;
    }

    public static void setServer(ObjectServer _server) {
        server = _server;
    }
    
    private void init(String fileName, int port, String uid, String psw) {

        ServerConfiguration config = Db4oClientServer.newServerConfiguration();
        config.common().add(new TransparentActivationSupport());
        config.common().add(new TransparentPersistenceSupport(new DeactivatingRollbackStrategy()));
        config.file().generateUUIDs(ConfigScope.GLOBALLY);
        config.file().generateCommitTimestamps(true);
//        config.file().

        setServer(Db4oClientServer.openServer(config, fileName, port));
        getServer().grantAccess(uid, psw);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (getServer() != null) {
                    getServer().close();
                }
            }
        });

    }
   
    // End Constructors
    //==========================================================================
    /**
     * 
     * @param container 
     */
    private void rollbackCloseAndClearObjectContainer(ObjectContainer container) {
        if (container != null) {
            container.rollback();
            container.close();
        }
    }

    @Override
    public synchronized void saveProcessingModel(ProcessingModel model) throws RepositoryException {
        
        checkArgument(model != null, "model cannot be null");
        checkArgument(getServer() != null, "server cannot be null");

        model.setLastSaved(DateTime.now());

        try {
            ObjectContainer client = getServer().openClient();

            LOG.debug("Saving model {}...", model.getModelName());

            client.store(model);
            client.commit();
            
            client.close();

            LOG.debug("Saving completed");
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        }
    }

    public synchronized void saveProcessingModel(ObjectContainer client, ProcessingModel model) throws RepositoryException {
        
        checkArgument(model != null, "model cannot be null");
        checkArgument(getServer() != null, "server cannot be null");

        model.setLastSaved(DateTime.now());

        try {
            
            LOG.debug("Saving model {}...", model.getModelName());

            client.store(model);
            client.commit();
            
            LOG.debug("Saving completed");
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        }
    }
    
    @Override
    public synchronized List<ProcessingModel> getProcessingModelsByName(String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");
        checkArgument(getServer() != null, "server cannot be null");
        
        final String query = createQuery(name);

        LOG.debug("Getting models like {}", query);
        
        return getResultSet(query);
    }

    public synchronized List<ProcessingModel> getProcessingModelsByName(ObjectContainer client, String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");
//        checkArgument(getServer() != null, "server cannot be null");
        
        final String query = createQuery(name);

        LOG.debug("Getting models like {}", query);
        
        return getResultSet(client, query);
    }
    
    @Override
    public ProcessingModel getProcessingModelByName(String name) throws RepositoryException {
        
        List<ProcessingModel> models = null;
        
        try {
            models = getProcessingModelsByName(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (models == null || models.isEmpty()) {
            return null;
        } else {
            ProcessingModel model = models.get(0);
            
            activateModel(model);

            return model;
        }
    }
    
    public ProcessingModel getProcessingModelByName(ObjectContainer client, String name) throws RepositoryException {
        
        List<ProcessingModel> models = null;
        
        try {
            models = getProcessingModelsByName(client, name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (models == null || models.isEmpty()) {
            return null;
        } else {
            ProcessingModel model = models.get(0);
            
            activateModel(model);

            return model;
        }
    }
            
    /**
     * 
     * @param model 
     */
    public synchronized ProcessingModel activateModel(ProcessingModel model) {
        
        Set<ExternalSource> externalSources = model.getExternalSources();
        for (ExternalSource externalSource : externalSources) {
            Output output = externalSource.getOutput();            
        }

        Set<Processor> processors = model.getProcessors();
        for (Processor processor : processors) {
            List<? extends Input> inputs = processor.getInputs();
            for (Input input : inputs) {
                Source source = input.getSource();
            }
        }

        Set<ExternalSink> externalSinks = model.getExternalSinks();
        for (ExternalSink externalSink : externalSinks) {
            List<? extends Input> inputs = externalSink.getInputs();
            for (Input input : inputs) {
                Source source = input.getSource();
            }
        } 
        
        return model;
    }  
    
    @Override
    public Set<ExternalSink> getExternalSinkTemplateByClassName(String name) throws RepositoryException {
        
        Set<ExternalSink> sinks = null;
        try {
            sinks = getExternalSinkTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (sinks == null || sinks.isEmpty()) {
            return null;
        } else {
//            ExternalSink sink = sinks.get(0);            
            return sinks;
        }
    }

    @Override
    public Set<ExternalSource> getExternalSourceTemplateByClassName(String name) throws RepositoryException {
        
        Set<ExternalSource> sources = null;
        try {
            sources = getExternalSourceTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (sources == null || sources.isEmpty()) {
            return null;
        } else {
//            ExternalSource source = sources.get(0);            
            return sources;
        }
    }

    @Override
    public Set<Processor> getProcessorTemplateByClassName(String name) throws RepositoryException {
        
        Set<Processor> processors = null;
        try {
            processors = getProcessorTemplates(name);
        } catch (RepositoryException ex) {
            Exceptions.printStackTrace(ex);
        }

        if (processors == null || processors.isEmpty()) {
            return null;
        } else {
//            Processor processor = processors.get(0);            
            return processors;
        }
    }

    private String createQuery(String name) {
        String query;
        if (name.trim().length() == 0) {
            query = ".*";

        } else {
            // make sure that the search string always ends in a '*' 
            if (name.endsWith("*")) {
                // then change from the "regular" '*' to the regex friendly '.*'
                query = StringUtils.replace(name, "*", ".*");

            } else {
                query = StringUtils.replace(name, "*", ".*") + ".*";
            }
        }
        return query;
    }
    
    /**
     * Extracts all external sinks that have name matched with name provided
     * @param name
     * @return
     * @throws RepositoryException 
     */
    @Override
    public Set<ExternalSink> getExternalSinkTemplates(String sinkName) throws RepositoryException {
        
        checkArgument(sinkName != null, "name cannot be null");
        checkArgument(getServer() != null, "server cannot be null");

        final String query;
        
        query = createQuery(sinkName);

        LOG.debug("Getting sinks like {}", query);

        try {
            ObjectContainer client = getServer().openClient();

            Set set = Sets.newHashSet(client.query(new Predicate<ExternalSink>() {
                @Override
                public boolean match(ExternalSink sink) {
                    return sink.getName().matches(query);
                }
            }).iterator());
            
            client.close();
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }

    @Override
    public Set<ExternalSource> getExternalSourceTemplates(String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");
        checkArgument(getServer() != null, "server cannot be null");

        final String query;
        
        query = createQuery(name);

        LOG.debug("Getting sources like {}", query);

        try {
            
            ObjectContainer client = getServer().openClient();

            Set set = Sets.newHashSet(client.query(new Predicate<ExternalSource>() {
                @Override
                public boolean match(ExternalSource source) {
                    return source.getName().matches(query);
                }
            }).iterator());
            
            client.close();
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }

    @Override
    public Set<Processor> getProcessorTemplates(String name) throws RepositoryException {
        
        checkArgument(name != null, "name cannot be null");
        checkArgument(getServer() != null, "server cannot be null");

        final String query;
        
        query = createQuery(name);

        LOG.debug("Getting processors like {}", query);

        try {
            ObjectContainer client = getServer().openClient();

            Set set = Sets.newHashSet(client.query(new Predicate<Processor>() {
                @Override
                public boolean match(Processor processor) {
                    return processor.getName().matches(query);
                }
            }).iterator());
            
            client.close();
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        }
    }

    /**
     * 
     * @param query
     * @return
     * @throws Db4oIOException
     * @throws RepositoryException 
     */
    private List<ProcessingModel> getResultSet(final String query) throws Db4oIOException, RepositoryException {
        
        ObjectContainer client = null;
        
        try {
            client = getServer().openClient();
            
            ObjectSet set = client.query(new Predicate<ProcessingModel>() {
                @Override
                public boolean match(ProcessingModel model) {
                    return model.getModelName().matches(query);
                }
            });
            
            while (set.hasNext()) {
                Object obj = set.next();
                client.ext().refresh(obj, 100);
            }            
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
    
    /**
     * 
     * @param url
     * @param port
     * @param query
     * @return
     * @throws Db4oIOException
     * @throws RepositoryException 
     */
    private List<ProcessingModel> getResultSet(ObjectContainer client, final String query) throws Db4oIOException, RepositoryException {
        
        try {
            
            ObjectSet set = client.query(new Predicate<ProcessingModel>() {
                @Override
                public boolean match(ProcessingModel model) {
                    return model.getModelName().matches(query);
                }
            });
            
            while (set.hasNext()) {
                Object obj = set.next();
                client.ext().refresh(obj, 100);
            }            
            
            return set;
            
        } catch (Db4oException e) {
            throw new RepositoryException(e);
        } catch (IllegalStateException e) {
            // this is thrown if the class structure is incompatible with the current structure
            throw new RepositoryException(e);
        } 
    }
    
}
