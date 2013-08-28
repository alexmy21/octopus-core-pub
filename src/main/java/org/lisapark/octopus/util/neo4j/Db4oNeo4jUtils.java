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
package org.lisapark.octopus.util.neo4j;


import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.lisapark.octopus.core.JsonUtils;
import org.lisapark.octopus.core.ModelBean;
import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.ProcessorBean;
import org.lisapark.octopus.core.processor.Processor;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.ExternalSource;
import org.lisapark.octopus.repository.RepositoryException;
import org.lisapark.octopus.repository.db4o.OctopusDb4oRepository;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.rest.graphdb.RestGraphDatabase;


/**
 *
 * @author Alex
 */
public class Db4oNeo4jUtils {  

    public static enum RelTypes implements RelationshipType {
        MODEL,          // Relations between model node and it's parts (sources, processors, sinks)
//        processor_rel,      // Rlations between processor and it's attributes and parameters
        ATTRIBUTE,      // Attribute based relations between cluster and it's models
        VALUE,          // Value based relations between cluster and it's models
        CONTEXT,        // Context formation relations
        CLUSTER         // Cluster relation
//        PROCESSOR, 
//        PROCESSORGRAPH
    }
    
    private static String db4oPath = "C:/Users/Alex/Documents/NetBeansProjects/2013-05-09_OctopusNeo4j"
            + "/octopus-eng-1.0-dist/octopus-designer/bin/octopus.db";
    
    private static String neo4jPath = "/home/alex/neo4j-community-1.9.2/data/graph.db";
    
    private RestGraphDatabase       graphDb;
    private GraphDatabaseService    graphDbService = null;
    
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    
    private static final String CLASS_NAME      = "className";
    private static final String AUTHOR_EMAIL    = "authorEmail";
    private static final String PROC_ID         = "procId";
    private static final String PROC_NAME       = "procName";
    private static final String PROC_TYPE       = "procType";
    private static final String DESCRIPTION     = "description";
    private static final String PROC_JSON       = "procJson";
    
    private static final String MODEL_NAME      = "modelName";
    private static final String MODEL_JSON      = "modelJson";
    
    private static final String SINKS           = "SINKS";
    private static final String SOURCES         = "SOURCES";
    private static final String PROCESSORS      = "PROCESSORS";
    private static final String MODELS          = "MODELS";
    private static final String CLUSTERS        = "CLUSTERS";
    
    private static final String MODEL_RELS      = "MODELS_RELS";
    private static final String CLUSTER_RELS    = "CLUSTERS_RELS";
    
    private static final String REL_NAME        = "name";
    
    private static final String CLUSTER_NAME    = "name";
    private static final String ROOT_CLUSTER    = "ROOT";
    
    // Set of traversal descriptions
    //==========================================================================
    final TraversalDescription SINKS_TRAVERSAL = Traversal.description()
                    .breadthFirst()
                    .relationships(Db4oNeo4jUtils.RelTypes.MODEL)
                    .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
    
    public synchronized Set<ModelBean> getSinkRelatedModels(GraphDatabaseService graphDb, String sinkClass) {
        
        Set<ModelBean> set = Sets.newHashSet();
        Node processorNode; // = null;

        IndexManager index = graphDb.index();
        Index<Node> processors = index.forNodes(SINKS);
        IndexHits<Node> hits = processors.get(CLASS_NAME, sinkClass);

        if (hits.size() == 0) {
            set = null;
        } else {
            processorNode = hits.getSingle();

            for (Node currentNode : SINKS_TRAVERSAL
                    //                .evaluator( Evaluators.fromDepth( 2 ) )
                    //                .evaluator( Evaluators.toDepth( 4 ) )
                    .traverse(processorNode)
                    .nodes()) {
               
                if (currentNode.hasProperty(ModelBean.MODEL_JSON)) {
                    String json = (String)currentNode.getProperty(ModelBean.MODEL_JSON);
                    ModelBean bean = new Gson().fromJson(json, ModelBean.class);
                    String sinkBeanJson = bean.getSinks().iterator().next();
                    ProcessorBean sinkBean = new Gson().fromJson(sinkBeanJson, ProcessorBean.class);
                    if (sinkClass.equalsIgnoreCase(sinkBean.getClassName())) {
                        set.add(bean);
                    }
                } else {
                    continue;
                }
            }
        }
        
        return set;
    }

    
    
    /**
     * 
     * @param args
     * @throws RepositoryException 
     */
    public static void main(String[] args) throws RepositoryException{
        
        Db4oNeo4jUtils utils = new Db4oNeo4jUtils();
       
        if (utils.getGraphDbService() == null) {
//            utils.setGraphDbService(utils.newEmbeddedInstance(neo4jPath));
            utils.setGraphDbService(utils.newServerInstance(SERVER_ROOT_URI));
        } else {
//                utils.setGraphDb(utils.getGraphDb());
//                Do nothing.
        }  

        OctopusDb4oRepository container = new OctopusDb4oRepository(db4oPath);        
         
        List<ProcessingModel> modelList = container.getProcessingModelsByName("");
       
//        showAllSinks(container);
//        
//        Transaction tx = graphDb.beginTx();
//        try {
            utils.addAllSinks(container, utils.getGraphDbService());
            utils.addAllSources(container, utils.getGraphDbService());
            utils.addAllProcessors(container, utils.getGraphDbService());

            utils.addModels(modelList, utils.getGraphDbService());
//            tx.success();
//        } finally {
//            tx.finish();
//        }
//        showAllSources(container);
//        showAllProcessors(container);

//        try {
        utils.showAllModels(modelList);
//        } catch (RepositoryException ex) {
//            Exceptions.printStackTrace(ex);
//        }

//        graphDb.shutdown();
        Runtime.getRuntime().exit(0);
               
    }

    /**
     *
     * @param neo4jUrl
     * @return
     */
    public synchronized GraphDatabaseService newEmbeddedInstance(String neo4jUrl) {
        return new GraphDatabaseFactory().newEmbeddedDatabase(neo4jUrl);
    }

    /**
     *
     * @param neo4jUrl
     * @return
     */
    public synchronized GraphDatabaseService newServerInstance(String neo4jUrl) {
        int status = checkDatabaseIsRunning(neo4jUrl);
        if(status == 200){
            return new RestGraphDatabase(neo4jUrl);
        } else {
            return null;
        }
    }
    
    /**
     * @return the graphDbService
     */
    public GraphDatabaseService getGraphDbService() {
        return graphDbService;
    }

    /**
     * @param graphDbService the graphDbService to set
     */
    public void setGraphDbService(GraphDatabaseService graphDbService) {
        this.graphDbService = graphDbService;
    }
    
    
    public synchronized Node addProcessor(GraphDatabaseService graphDb, String procType, String json){
        
        String indexName;
        ProcessorBean bean = new Gson().fromJson(json, ProcessorBean.class);
        
        if(procType.equalsIgnoreCase(JsonUtils.SINK)){
            indexName = SINKS;
        } else if(procType.equalsIgnoreCase(JsonUtils.SOURCE)){
            indexName = SOURCES;
        } else {
            indexName = PROCESSORS;
        }
        return addProcessor(graphDb, indexName, bean, json);
        
    }
    
    public synchronized Node addProcessor(GraphDatabaseService graphDb, String indexName, ProcessorBean bean, String json) {
        
        Node processorNode = null;

        IndexManager index = graphDb.index();
        Index<Node> processors = index.forNodes(indexName);
        IndexHits<Node> hits = processors.get(CLASS_NAME, bean.getClassName());

        if (hits.size() == 0) {
            Transaction tx = graphDb.beginTx();
            try {
                processorNode = graphDb.createNode();
                processorNode.setProperty(PROC_ID, bean.getProcId());
                processorNode.setProperty(CLASS_NAME, bean.getClassName());
                processorNode.setProperty(AUTHOR_EMAIL, bean.getAuthorEmail());
                processorNode.setProperty(PROC_NAME, bean.getName());
                processorNode.setProperty(PROC_TYPE, bean.getProcType());
                processorNode.setProperty(DESCRIPTION, bean.getDescription());
                processorNode.setProperty(PROC_JSON, json);

                processors.add(processorNode, CLASS_NAME, bean.getClassName());
                tx.success();
            } finally {
                tx.finish();
            }
        } else {
            processorNode = hits.getSingle();
        }

        return processorNode;
    }
    
    /**
     * 
     * @param container
     * @param graphDb 
     */
    private void addAllSinks(OctopusDb4oRepository container, GraphDatabaseService graphDb){
        
        List<ExternalSink> sinkList = container.getAllExternalSinkTemplates();
        
        for (ExternalSink sink : sinkList) {
            String json = sink.toJson();
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            addProcessor(graphDb, SINKS, bean, json);
        }
    }
   
    /**
     * 
     * @param container
     * @param graphDb 
     */
    private void addAllSources(OctopusDb4oRepository container, GraphDatabaseService graphDb){
        
        List<ExternalSource> sourceList = container.getAllExternalSourceTemplates();
        
        for (ExternalSource source : sourceList) {
            String json = source.toJson();
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            addProcessor(graphDb, SOURCES, bean, json);
        }
    }
    
    /**
     * 
     * @param container
     * @param graphDb 
     */
    private void addAllProcessors(OctopusDb4oRepository container, GraphDatabaseService graphDb){
        
        List<Processor> processorList = container.getAllProcessorTemplates();
        
        for (Processor processor : processorList) {
            String json = processor.toJson();
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            addProcessor(graphDb, PROCESSORS, bean, json);
        }
    }
    
    
    private void addModels(List<ProcessingModel> modelList, GraphDatabaseService graphDb) {
       
        for (Iterator<ProcessingModel> it = modelList.iterator(); it.hasNext();) {
            ProcessingModel model = it.next();
            
            String json = model.toJson();
            addModelContext2Cluster(json, graphDb);
        }
    }
        
    /**
     * @return the graphDb
     */
    public RestGraphDatabase getGraphDb() {
        return graphDb;
    }

    /**
     * @param graphDb the graphDb to set
     */
    public void setGraphDb(RestGraphDatabase graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * 
     * @param json
     * @param graphDb
     * @throws JsonSyntaxException 
     */
    public synchronized void addModelContext2Cluster(String json, GraphDatabaseService graphDb) throws JsonSyntaxException {
        ModelBean bean = (ModelBean) new Gson().fromJson(json, ModelBean.class);
        
        Node modelNode = addModel(graphDb, bean, json);
        
        if (modelNode != null) {
            addModelRelations(graphDb, modelNode, bean);
            addModel2Cluster(graphDb, json, modelNode);
        }
    }
    
    public synchronized Node addModel(GraphDatabaseService graphDb, 
            ModelBean modelBean, String json){
        
        Node modelNode = null;
        
        IndexManager nodeIndex = graphDb.index();
        Index<Node> models = nodeIndex.forNodes(MODELS);
        
        IndexHits<Node> hits = models.get(MODEL_NAME, modelBean.getModelName());

        if (hits.size() == 0) {
            Transaction tx = graphDb.beginTx();
            try {
                modelNode = graphDb.createNode();
                modelNode.setProperty(MODEL_NAME, modelBean.getModelName());
                modelNode.setProperty(AUTHOR_EMAIL, modelBean.getAuthorEmail());
                modelNode.setProperty(DESCRIPTION, modelBean.getDescription());
                modelNode.setProperty(MODEL_JSON, json);

                models.add(modelNode, MODEL_NAME, modelBean.getModelName());
                tx.success();
            } finally {
                tx.finish();
            }
        } else {
            modelNode = hits.getSingle();
        }
        return modelNode;
    }
    
    /**
     * 
     */
    public synchronized void addModelRelations(GraphDatabaseService graphDb, Node modelNode, ModelBean bean) {
         
        Set<String> sinks = bean.getSinks();
        for (String sinkStr : sinks) {
            createModelRelation(graphDb, sinkStr, SINKS, bean, modelNode);
        }
        
        Set<String> sources = bean.getSources();
        for (String sourceStr : sources) {
            createModelRelation(graphDb, sourceStr, SOURCES, bean, modelNode);
        }
        
        Set<String> processors = bean.getProcessors();
        for (String processorStr : processors) {
            createModelRelation(graphDb, processorStr, PROCESSORS, bean, modelNode);
        }
    }
    
    /**
     * 
     * @param modelRels
     * @param procs
     * @param nodes
     * @param bean
     * @param modelNode
     * @throws JsonSyntaxException 
     */    
    public synchronized void createModelRelation(GraphDatabaseService graphDb, String procStr, String nodeType, ModelBean bean, Node modelNode)
            throws JsonSyntaxException {

        ProcessorBean procBean = new Gson().fromJson(procStr, ProcessorBean.class);
        
        IndexManager nodeIndex = graphDb.index();
        IndexManager relIndex = graphDb.index();
        Index<Relationship> modelRels = relIndex.forRelationships(MODEL_RELS);
        Index<Node> nodes = nodeIndex.forNodes(nodeType);
        
        IndexHits<Node> hits = nodes.get(CLASS_NAME, procBean.getClassName());

        Node procNode;
        if (hits.size() == 0) {
            procNode = addProcessor(graphDb, nodeType, procBean, procStr);            
        } else {
            procNode = hits.getSingle();
        }
        // Check if relationship is in the index and add it, if it is not there yet
        IndexHits<Relationship> relHits = modelRels.get(CLASS_NAME, procBean.getClassName());
        if (relHits.size() == 0 && procNode != null) { 
            Transaction tx = graphDb.beginTx();
            try {
                Relationship rel = procNode.createRelationshipTo(modelNode, RelTypes.MODEL);
                rel.setProperty(CLASS_NAME, procBean.getClassName());
                rel.setProperty(REL_NAME, bean.getModelName());
                modelRels.add(rel, REL_NAME, bean.getModelName());
                
                tx.success();
            } finally {
                tx.finish();
            }
        }
    }
    
    /**
     * 
     * @param graphDb
     * @param clusters
     * @param bean
     * @param json
     * @param modelNode 
     */
    public void addModel2Cluster(GraphDatabaseService graphDb, String json, Node modelNode) {
        
        ModelBean modelBean = new Gson().fromJson(json, ModelBean.class);
        
        IndexManager nodeIndex = graphDb.index();
        IndexManager clusterRelIndex = graphDb.index();
        Index<Relationship> clusterRels = clusterRelIndex.forRelationships(CLUSTER_RELS);
        Index<Node> clusters = nodeIndex.forNodes(CLUSTERS);
        
        IndexHits<Node> hits = clusters.get(CLUSTER_NAME, ROOT_CLUSTER);

        Transaction tx = graphDb.beginTx();
        try {
            Node clusterNode;
            if (hits.size() == 0) {
                clusterNode = graphDb.createNode();
                clusterNode.setProperty(CLUSTER_NAME, ROOT_CLUSTER);
                clusters.add(clusterNode, CLUSTER_NAME, ROOT_CLUSTER);
            } else {
                clusterNode = hits.getSingle();
            }
            // Check if relationship is in the index and add it, if it is not there yet
            IndexHits<Relationship> relHits = clusterRels.get(REL_NAME, modelBean.getModelName());
            if (relHits.size() == 0 && clusterNode != null) {
                Relationship rel = modelNode.createRelationshipTo(clusterNode, RelTypes.CLUSTER);
                rel.setProperty(REL_NAME, modelBean.getModelName());
                clusterRels.add(rel, REL_NAME, modelBean.getModelName());
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }
        
    /**
     * 
     */
    public int checkDatabaseIsRunning(String server_uri) {
        // START SNIPPET: checkServer
        WebResource resource = Client.create().resource(server_uri);
        
        ClientResponse response = resource.get(ClientResponse.class);

        int status = response.getStatus();
        System.out.println(String.format("GET on [%s], status code [%d]",
                server_uri, status));
        response.close();
        // END SNIPPET: checkServer
        return status;
    }
    
    private void showAllSinks(OctopusDb4oRepository container) throws RepositoryException {
        List<ExternalSink> sinkList = container.getAllExternalSinkTemplates();
        
        for(ExternalSink sink : sinkList){
            String json = sink.toJson();
            System.out.println(json);
            // Do the same calling db4o
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            Set<ExternalSink> extSinks = container.getExternalSinkTemplateByClassName(bean.getClassName());
            if(extSinks == null){
                json = "Models with Sink: " + bean.getClassName() + " do not exist.";
                System.out.println("ExtProcessor ===> " + json);
            } else {
                for (ExternalSink extSink : extSinks) {
                    json = extSink.toJson();
                    ProcessorBean m_bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
                    System.out.println("Tolerance ===> " + bean.compare(m_bean));
                    System.out.println("ExtSink ===> " + json);
                }
            }
            
        }
    }
    
    private void showAllSources(OctopusDb4oRepository container) throws RepositoryException {
        List<ExternalSource> sourceList = container.getAllExternalSourceTemplates();
        
        for(ExternalSource source : sourceList){
            String json = source.toJson();
            System.out.println(json);
            
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            Set<ExternalSource> extSources = container.getExternalSourceTemplateByClassName(bean.getClassName());
            if(extSources == null){
                json = "Models with Source: " + bean.getClassName() + " do not exist.";
                System.out.println("ExtProcessor ===> " + json);
            } else {
                for (ExternalSource extSource : extSources) {
                    json = extSource.toJson();
                    ProcessorBean m_bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
                    System.out.println("Tolerance ===> " + bean.compare(m_bean));
                    System.out.println("ExtSource ===> " + json);
                }
            }
            
        }
    }
    
    private void showAllProcessors(OctopusDb4oRepository container) throws RepositoryException {
        List<Processor> processorList = container.getAllProcessorTemplates();
        
        for(Processor processor : processorList){ 
            String json = processor.toJson();
            System.out.println(json);
            
            ProcessorBean bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
            Set<Processor> extProcessors = container.getProcessorTemplateByClassName(bean.getClassName());
            if(extProcessors == null){
                json = "Models with Processor: " + bean.getClassName() + " do not exist.";
                System.out.println("ExtProcessor ===> " + json);
            } else {
                for (Processor extProcessor : extProcessors) {
                    json = extProcessor.toJson();
                    ProcessorBean m_bean = (ProcessorBean) new Gson().fromJson(json, ProcessorBean.class);
                    System.out.println("Tolerance ===> " + bean.compare(m_bean));
                    System.out.println("ExtProcessor ===> " + json);
                }
            }
            
        }
    }
    
    private void showAllModels(List<ProcessingModel> modelList) throws RepositoryException {
//        List<ProcessingModel> modelList = container.getProcessingModelsByName(name);
        ProcessingModel model0 = modelList.get(0);
        ModelBean m_model0 = new Gson().fromJson(model0.toJson(), ModelBean.class);
        
        for (Iterator<ProcessingModel> it = modelList.iterator(); it.hasNext();) {
            ProcessingModel model = it.next();

            String json = model.toJson();
            System.out.println(json);
            
            ModelBean m_model = new Gson().fromJson(json, ModelBean.class);
            
            System.out.println("Tolerance               ===> " + m_model0.compare(m_model));
            System.out.println("Tolerance by sources    ===> " + m_model0.compareBySources(m_model));
            System.out.println("Tolerance by sinks      ===> " + m_model0.compareBySinks(m_model));
            System.out.println("Tolerance by processors ===> " + m_model0.compareByProcessors(m_model));
                      
        }
    }
    
}
