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
package org.lisapark.octopus.util.cpneo4j;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Constraint Programming (Cp) with Neo4j graph database contains methods that
 * provide support for conversion of CP Optimization task JSON description to
 * Neo4j database and from Neo4j to Constraint Model and Solution.
 *
 *
 * @author Alex
 */
public class CpNeo4jUtils {    

    /**
     * @return the graphDbService
     */
    public GraphDatabaseService getGraphDbService() {
        return graphDbService;
    }

    // Relations
    //==========================================================================
    
    public static enum RelTypes implements RelationshipType {
        PRODUCT,    // Relations between product node and it's parts (technology steps)
        T_PROCESS,  // Relations between technological steps
        MACHINE,    // Relation between machine and technological steps that use this machine
        SOLUTION,
        ATTRIBUTE,  // Attribute based relations between cluster and it's models
        VALUE,      // Value based relations between cluster and it's models
        CONTEXT,    // Context formation relations
        CLUSTER     // Cluster relation
    }
    
    // Traversal Descriptions
    //==========================================================================
    
    final TraversalDescription SOLUTIONS_TRAVERSAL = Traversal.description()
        .breadthFirst()
        .relationships( RelTypes.SOLUTION )
        .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
    
    final TraversalDescription PRODUCTS_TRAVERSAL = Traversal.description()
        .breadthFirst()
        .relationships( RelTypes.PRODUCT )
        .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
    
    final TraversalDescription MACHINES_TRAVERSAL = Traversal.description()
        .breadthFirst()
        .relationships( RelTypes.MACHINE )
        .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
    
    // Constants
    //==========================================================================
    
    private static final String SERVER_ROOT_URI     = "http://localhost:7474/db/data/";

    public static final String TECHNOLOGY_STEPS     = "TECHNOLOGY_STEPS";
    public static final String MACHINES             = "MACHINES";
    public static final String PRODUCTS             = "PRODUCTS";
    public static final String SOLUTIONS            = "SOLUTIONS";
    
    public static final String MACHINE_TECH_STEPS_RELS  = "MACHINE_TECH_STEPS_RELS";
    public static final String PRODUCT_TECH_STEPS_RELS  = "PRODUCT_TECH_STEPS_RELS";
    public static final String TECH_PROCESS_RELS        = "TECH_PROCESS_RELS";
    
    public static final String SOLUTION_RELS        = "SOLUTION_RELS";
    
    public static final String REL_NAME             = "name";
    public static final String NODE_TYPE            = "NODE_TYPE";
    
    public static final String SOLUTION_NAME        = "name";
    public static final String ROOT_SOLUTION        = "ROOT_SOLUTION";
    
    public static final String QUERY_TYPE           = "QUERY_TYPE";
    public static final String TYPE_ID              = "TYPE_ID";
    public static final String JSON                 = "JSON";
    
    public static final String PRODUCT              = "PRODUCT";
    public static final String MACHINE              = "MACHINE";
    public static final String ALL                  = "ALL";
    
    // Variables
    //==========================================================================
    
    private GraphDatabaseService graphDbService     = null;
    
    // Methods
    //==========================================================================
    
    /**
     * 
     * @param args 
     */
    public static void main(String[] args){
        
        CpNeo4jUtils utils = new CpNeo4jUtils();
        
        GraphDatabaseService graphDb = utils.newServerInstance(SERVER_ROOT_URI);
        
        
        // Exit runtime
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
    public synchronized GraphDatabaseService newServerInstance(String neo4jUrl, String userId, String pwd) {
        int status = checkDatabaseIsRunning(neo4jUrl);
        if(status == 200){
            return new RestGraphDatabase(neo4jUrl);
        } else {
            return null;
        }
    }
    
    public synchronized GraphDatabaseService newServerInstance(String neo4jUrl){
        return newServerInstance(neo4jUrl, "", "");
    }
    
    /**
     *
     */
    public synchronized int     checkDatabaseIsRunning(String server_uri) {
        // START SNIPPET: checkServer
        WebResource resource = Client.create()
                .resource(server_uri);
        ClientResponse response = resource.get(ClientResponse.class);

        int status = response.getStatus();
        System.out.println(String.format("GET on [%s], status code [%d]",
                server_uri, status));
        response.close();
        // END SNIPPET: checkServer
        return status;
    }
    
    /**
     * @param graphDbService the graphDbService to set
     */
    public void                 setGraphDbService(GraphDatabaseService graphDbService) {
        this.graphDbService = graphDbService;
    }

    /**
     * 
     * @param graphDb
     * @param typeId
     * @return 
     */
    public Set<Map<String, String>> getProductTechSteps(GraphDatabaseService graphDb, String typeId) {
        return getTechSteps(graphDb, typeId, Product.PRODUCT_ID);
    }

    /**
     * 
     * @param graphDb
     * @param typeId
     * @return 
     */
    public Set<Map<String, String>> getMachineTechSteps(GraphDatabaseService graphDb, String typeId) {        
        return getTechSteps(graphDb, typeId, MachineResource.MACHINE_RESOURCE_ID);
    }
    
    /**
     * 
     * @param graphDb
     * @param typeId
     * @param typeIdName
     * @param query
     * @return 
     */
    public Set<Map<String, String>> getTechSteps(GraphDatabaseService graphDb, String typeId, String typeIdName) {
         
        Set<Map<String, String>> set = Sets.newHashSet();
        
        IndexManager index = graphDb.index();
        Index<Node> technologySteps = index.forNodes(TECHNOLOGY_STEPS);
        
        IndexHits<Node> hits;
        if(!typeIdName.isEmpty() && !typeId.isEmpty()){
            hits = technologySteps.get(typeIdName, typeId);            
        } else if(!typeIdName.isEmpty() && typeId.isEmpty()){
            hits = technologySteps.query(typeIdName, "*");
        } else {
            hits = technologySteps.query(TechnologyStep.TECHNOLOGY_STEP_ID, "*");
        }
        
        Iterator<Node> iterator = hits.iterator();
        while (iterator.hasNext()) {            
            Node currentNode = iterator.next();
            Map<String, String> map = Maps.newHashMap();
            
            if(currentNode == null || !currentNode.hasProperty(TechnologyStep.TECHNOLOGY_STEP_JSON)){
                continue;
            } else {
                String queryType = typeIdName.trim().isEmpty() ? 
                        TechnologyStep.TECHNOLOGY_STEP_ID : typeIdName.trim();
                map.put(QUERY_TYPE, queryType);
                
                String techStepTypeId = (String) currentNode.getProperty(queryType);
                map.put(TYPE_ID, techStepTypeId);
                
                String nodeJson = (String) currentNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);
                map.put(JSON, nodeJson);
                
                set.add(map);
            }            
        }        
        
        return set;
    }
    
    /**
     * 
     * @param graphDb
     * @return 
     */
    public Set<Map<String, Object>> getAllProducts(GraphDatabaseService graphDb) {
        Set<Map<String, Object>> set = Sets.newHashSet();
        
        IndexManager index      = graphDb.index();
        Index<Node> products    = index.forNodes(PRODUCTS);
        
        IndexHits<Node> hits;
        hits = products.query(Product.PRODUCT_ID, "*");
               
        Iterator<Node> iterator = hits.iterator();
        while (iterator.hasNext()) {            
            Node currentNode = iterator.next();
            Map<String, Object> map = Maps.newHashMap();
            
            if(currentNode == null || !currentNode.hasProperty(Product.PRODUCT_JSON)){
                continue;
            } else {
                String nodeJson = (String) currentNode.getProperty(Product.PRODUCT_JSON);
                Product product = new Gson().fromJson(nodeJson, Product.class);
                map.put(Product.PRODUCT_ID, product.getProductId());
                map.put(Product.PRODUCT_NAME, product.getProductName());
                map.put(Product.PRODUCT_PRICE, product.getProductPrice());
                map.put(Product.PRODUCTION_LOW, product.getProductionLowBound());
                map.put(Product.PRODUCTION_UPPER, product.getProductionUpperBound());
                map.put(Product.PRODUCTION_VALUE, product.getProductionValue());
                map.put(Product.DESCRIPTION, product.getDescription());
                map.put(Product.PRODUCT_JSON, nodeJson);

                set.add(map);
            }            
        }          
        return set;
    }

    /**
     * 
     * @param graphDb
     * @return 
     */
    public Set<Map<String, Object>> getAllMachines(GraphDatabaseService graphDb) {
        Set<Map<String, Object>> set = Sets.newHashSet();
        
        IndexManager index      = graphDb.index();
        Index<Node> machines    = index.forNodes(MACHINES);
        
        IndexHits<Node> hits;
        hits = machines.query(MachineResource.MACHINE_RESOURCE_ID, "*");
               
        Iterator<Node> iterator = hits.iterator();
        while (iterator.hasNext()) {            
            Node currentNode = iterator.next();
            Map<String, Object> map = Maps.newHashMap();
            
            if(currentNode == null || !currentNode.hasProperty(MachineResource.MACHINE_JSON)){
                continue;
            } else {
                String nodeJson = (String) currentNode.getProperty(MachineResource.MACHINE_JSON);
                MachineResource machine = new Gson().fromJson(nodeJson, MachineResource.class);
                map.put(MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());
                map.put(MachineResource.MACHINE_NAME, machine.getMachineName());
                map.put(MachineResource.RESOURCE_UNIT_COST, machine.getResourceUnitCost());
                map.put(MachineResource.LOW_BOUND, machine.getLowBound());
                map.put(MachineResource.UPPER_BOUND, machine.getUpperBound());
                map.put(MachineResource.RESOURCE_USED, machine.getResourceUsed());
                map.put(MachineResource.RESOURCE_TOTAL, machine.getResourceTotal());
                map.put(MachineResource.MEASURE_UNIT, machine.getMeasureUnit());
                map.put(MachineResource.DESCRIPTION, machine.getDescription());
                map.put(MachineResource.MACHINE_JSON, nodeJson);

                set.add(map);
            }            
        }          
        return set;
    }
    
    /**
     * 
     * @param productList
     * @param graphDb 
     */
    private void                addAllProducts(GraphDatabaseService graphDb, List<Product> productList) {

        for (Product product : productList) {
            String json = new Gson().toJson(product, Product.class);            
            addProduct(graphDb, product, json);
        }
    }
    
    /**
     * 
     * @param graphDb
     * @param indexName
     * @param product
     * @param json
     * @return 
     */ 
    public synchronized Node    addProduct(GraphDatabaseService graphDb, Product product, String json) {
        
        Node productNode = null;

        IndexManager index = graphDb.index();
        Index<Node> products = index.forNodes(PRODUCTS);
        IndexHits<Node> hits = products.get(Product.PRODUCT_ID, product.getProductId());

        Transaction tx = graphDb.beginTx();
        try {
            if (hits.size() == 0) {
                productNode = graphDb.createNode();
                setProductProperties(productNode, product, json);
                products.add(productNode, Product.PRODUCT_ID, product.getProductId());
            } else {
                productNode = hits.getSingle();
                String jsonString = (String) productNode.getProperty(Product.PRODUCT_JSON);
                Product oldProduct = new Gson().fromJson(jsonString, Product.class);
                products.remove(productNode, Product.PRODUCT_ID, oldProduct.getProductId());
                setProductProperties(productNode, product, json);
                products.add(productNode, Product.PRODUCT_ID, product.getProductId());
            }
            tx.success();
        } finally {
            tx.finish();
        }

        return productNode;
    }    
    
    /**
     * 
     * @param productNode
     * @param product
     * @param json 
     */
    public void                 setProductProperties(Node productNode, Product product, String json) {
        productNode.setProperty(Product.PRODUCT_ID,         product.getProductId());
        productNode.setProperty(Product.PRODUCT_NAME,       product.getProductName());
        productNode.setProperty(Product.PRODUCT_PRICE,      product.getProductPrice());
        productNode.setProperty(Product.PRODUCTION_LOW,     product.getProductionLowBound());
        productNode.setProperty(Product.PRODUCTION_UPPER,   product.getProductionUpperBound());
        productNode.setProperty(Product.PRODUCTION_VALUE,   product.getProductionValue());
        productNode.setProperty(Product.DESCRIPTION,        product.getDescription());
        productNode.setProperty(Product.PRODUCT_JSON,       json);
    }

    /**
     * 
     * @param graphDb
     * @param productJson
     * @return 
     */
    public synchronized Node    createProductRels(GraphDatabaseService graphDb, String productJson){
         
        Product product = new Gson().fromJson(productJson, Product.class);
        
        IndexManager nodeIndex = graphDb.index();
        IndexManager relIndex = graphDb.index();
        Index<Relationship> productRels = relIndex.forRelationships(PRODUCT_TECH_STEPS_RELS);
        
        Index<Node> prodNodes = nodeIndex.forNodes(PRODUCTS);
        Index<Node> nodes = nodeIndex.forNodes(TECHNOLOGY_STEPS);
        
        IndexHits<Node> prodHits = prodNodes.get(Product.PRODUCT_ID, product.getProductId());
        
        Node node; 
        if (prodHits.size() == 0) {
            return null;            
        } else {
            node = prodHits.getSingle();
        }
        
        IndexHits<Node> hits = nodes.get(Product.PRODUCT_ID, product.getProductId());

        if (hits.size() == 0) {
            return null;            
        } 
        
        Iterator<Node> iterator = hits.iterator();
        while (iterator.hasNext()) {            
            Node currentNode = iterator.next();
            String nodeJson; 
            if(currentNode == null || !currentNode.hasProperty(TechnologyStep.TECHNOLOGY_STEP_JSON)){
                continue;
            } else {
                nodeJson = (String) currentNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON); 
            }
            
            TechnologyStep techStep = new Gson().fromJson(nodeJson, TechnologyStep.class);
            // Check if relationship is in the index and add it, if it is not there yet
            IndexHits<Relationship> relHits = productRels.get(TechnologyStep.TECHNOLOGY_STEP_ID, 
                    techStep.getTechnologyStepId());
            if (relHits.size() == 0) {
                Transaction tx = graphDb.beginTx();
                try {
                    Relationship rel = currentNode.createRelationshipTo(node, RelTypes.PRODUCT);
                    rel.setProperty(TechnologyStep.TECHNOLOGY_STEP_ID, techStep.getTechnologyStepId());
                    rel.setProperty(REL_NAME, product.getProductId());
                    productRels.add(rel, REL_NAME, product.getProductId());

                    tx.success();
                } finally {
                    tx.finish();
                }
            }
        }
        
        return node;
    }
    
    /**
     * 
     * @param graph
     * @param node
     * @return 
     */
    public Node                 createProductNodeSolutionRels(GraphDatabaseService graph, Node node){
        String json = (String) node.getProperty(Product.PRODUCT_JSON);
        Product product = new Gson().fromJson(json, Product.class);
        
        IndexManager nodeIndex = graph.index();
        IndexManager clusterRelIndex = graph.index();
        Index<Relationship> solutionRels = clusterRelIndex.forRelationships(SOLUTION_RELS);
        Index<Node> solutions = nodeIndex.forNodes(SOLUTIONS);
        
        IndexHits<Node> hits = solutions.get(SOLUTION_NAME, ROOT_SOLUTION);

        Transaction tx = graph.beginTx();
        try {
            Node solutionNode;
            if (hits.size() == 0) {
                solutionNode = graph.createNode();
                solutionNode.setProperty(SOLUTION_NAME, ROOT_SOLUTION);
                solutions.add(solutionNode, SOLUTION_NAME, ROOT_SOLUTION);
            } else {
                solutionNode = hits.getSingle();
            }
            // Check if relationship is in the index and add it, if it is not there yet
            IndexHits<Relationship> relHits = solutionRels.get(REL_NAME, product.getProductId());
            if (relHits.size() == 0 && solutionNode != null) {
                Relationship rel = node.createRelationshipTo(solutionNode, RelTypes.SOLUTION);
                rel.setProperty(NODE_TYPE, PRODUCT);
                rel.setProperty(REL_NAME, product.getProductId());
                solutionRels.add(rel, REL_NAME, product.getProductId());
            }
            tx.success();
        } finally {
            tx.finish();
        }
        return node;
    }
    
    /**
     * 
     * @param graph
     * @param node
     * @return 
     */
    public Node                 createMachineNodeSolutionRels(GraphDatabaseService graph, Node node){
        String json = (String) node.getProperty(MachineResource.MACHINE_JSON);
        MachineResource machine = new Gson().fromJson(json, MachineResource.class);
        
        IndexManager nodeIndex = graph.index();
        IndexManager clusterRelIndex = graph.index();
        Index<Relationship> solutionRels = clusterRelIndex.forRelationships(SOLUTION_RELS);
        Index<Node> solutions = nodeIndex.forNodes(SOLUTIONS);
        
        IndexHits<Node> hits = solutions.get(SOLUTION_NAME, ROOT_SOLUTION);

        Transaction tx = graph.beginTx();
        try {
            Node solutionNode;
            if (hits.size() == 0) {
                solutionNode = graph.createNode();
                solutionNode.setProperty(SOLUTION_NAME, ROOT_SOLUTION);
                solutions.add(solutionNode, SOLUTION_NAME, ROOT_SOLUTION);
            } else {
                solutionNode = hits.getSingle();
            }
            // Check if relationship is in the index and add it, if it is not there yet
            IndexHits<Relationship> relHits = solutionRels.get(REL_NAME, machine.getMachineResourceId());
            if (relHits.size() == 0 && solutionNode != null) {
                Relationship rel = node.createRelationshipTo(solutionNode, RelTypes.SOLUTION);
                rel.setProperty(NODE_TYPE, MACHINE);
                rel.setProperty(REL_NAME, machine.getMachineResourceId());
                solutionRels.add(rel, REL_NAME, machine.getMachineResourceId());
            }
            tx.success();
        } finally {
            tx.finish();
        }
        return node;
    }
    
    /**
     * 
     * @param machineList
     * @param graphDb 
     */
    private void                addAllMachines(GraphDatabaseService graphDb, List<MachineResource> machineList) {

        for (MachineResource machine : machineList) {
            String json = new Gson().toJson(machine, MachineResource.class);            
            addMachine(graphDb, machine, json);
        }
    }
    
    /**
     * 
     * @param graphDb
     * @param indexName
     * @param machine
     * @param json
     * @return 
     */ 
    public synchronized Node    addMachine(GraphDatabaseService graphDb, MachineResource machine, String json) {
        
        Node machineNode = null;

        IndexManager index = graphDb.index();
        Index<Node> machines = index.forNodes(MACHINES);
        IndexHits<Node> hits = machines.get(MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());

        Transaction tx = graphDb.beginTx();
        try {
            if (hits.size() == 0) {
                machineNode = graphDb.createNode();
                setMachineProperties(machineNode, machine, json);
                machines.add(machineNode, MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());
            } else {
                machineNode = hits.getSingle();
                String jsonString = (String) machineNode.getProperty(MachineResource.MACHINE_JSON);
                MachineResource oldMachine = new Gson().fromJson(jsonString, MachineResource.class);
                machines.remove(machineNode, MachineResource.MACHINE_RESOURCE_ID, oldMachine.getMachineResourceId());
                setMachineProperties(machineNode, machine, json);
                machines.add(machineNode, MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());
            }
            tx.success();
        } finally {
            tx.finish();
        }

        return machineNode;
    }
    
    /**
     * 
     * @param machineNode
     * @param machine
     * @param json 
     */
    public void                 setMachineProperties(Node machineNode, MachineResource machine, String json) {
        machineNode.setProperty(MachineResource.DESCRIPTION,        machine.getDescription());
        machineNode.setProperty(MachineResource.LOW_BOUND,          machine.getLowBound());
        machineNode.setProperty(MachineResource.MACHINE_RESOURCE_ID,machine.getMachineResourceId());
        machineNode.setProperty(MachineResource.MACHINE_NAME,       machine.getMachineName());
        machineNode.setProperty(MachineResource.MEASURE_UNIT,       machine.getMeasureUnit());
        machineNode.setProperty(MachineResource.RESOURCE_TOTAL,     machine.getResourceTotal());
        machineNode.setProperty(MachineResource.RESOURCE_UNIT_COST, machine.getResourceUnitCost());
        machineNode.setProperty(MachineResource.RESOURCE_USED,      machine.getResourceUsed());
        machineNode.setProperty(MachineResource.UPPER_BOUND,        machine.getUpperBound());
        machineNode.setProperty(MachineResource.MACHINE_JSON,       json);
    }
    
    /**
     * 
     * @param graphDb
     * @param node
     * @param machineJson 
     */
    public synchronized Node    createMachineRels(GraphDatabaseService graphDb, String machineJson){
        
        MachineResource machine = new Gson().fromJson(machineJson, MachineResource.class);
        
        IndexManager nodeIndex = graphDb.index();
        IndexManager relIndex = graphDb.index();
        Index<Relationship> machinelRels = relIndex.forRelationships(MACHINE_TECH_STEPS_RELS);
        Index<Node> nodes = nodeIndex.forNodes(TECHNOLOGY_STEPS);
        
        Index<Node> machineNodes = nodeIndex.forNodes(MACHINES);
        
        IndexHits<Node> machineHits = machineNodes.get(MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());
        
        Node node; 
        if (machineHits.size() == 0) {
            return null;            
        } else {
            node = machineHits.getSingle();
        }
        
        IndexHits<Node> hits = nodes.get(MachineResource.MACHINE_RESOURCE_ID, machine.getMachineResourceId());

        if (hits.size() == 0) {
            return null;            
        } 
        
        Iterator<Node> iterator = hits.iterator();
        while (iterator.hasNext()) {            
            Node currentNode = iterator.next();
            String nodeJson; 
            if(currentNode == null || !currentNode.hasProperty(TechnologyStep.TECHNOLOGY_STEP_JSON)){
                continue; 
            } else {
                nodeJson = (String) currentNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);                
            }
            
            TechnologyStep techStep = new Gson().fromJson(nodeJson, TechnologyStep.class);
            // Check if relationship is in the index and add it, if it is not there yet
            IndexHits<Relationship> relHits = machinelRels.get(TechnologyStep.TECHNOLOGY_STEP_ID, 
                    techStep.getTechnologyStepId());
            if (relHits.size() == 0) {
                Transaction tx = graphDb.beginTx();
                try {
                    Relationship rel = currentNode.createRelationshipTo(node, RelTypes.MACHINE);
                    rel.setProperty(TechnologyStep.TECHNOLOGY_STEP_ID, techStep.getTechnologyStepId());
                    rel.setProperty(REL_NAME, machine.getMachineResourceId());
                    machinelRels.add(rel, REL_NAME, machine.getMachineResourceId());

                    tx.success();
                } finally {
                    tx.finish();
                }
            }
        }
        return node;
    }

    /**
     * 
     * @param techStepList
     * @param graphDb 
     */
    private void                addAllTechnologySteps(GraphDatabaseService graphDb, List<TechnologyStep> techStepList) {
        for (TechnologyStep techStep : techStepList) {
            String json = new Gson().toJson(techStep, TechnologyStep.class);            
            addTechnologyStep(graphDb, techStep, json);
        }
    }
    
    /**
     * 
     * @param graphDb
     * @param technologyStep
     * @param json
     * @return 
     */
    public synchronized Node    addTechnologyStep(GraphDatabaseService graphDb, TechnologyStep technologyStep, String json) {
        
        Node techStepNode = null;

        IndexManager index = graphDb.index();
        Index<Node> technologySteps = index.forNodes(TECHNOLOGY_STEPS);
        IndexHits<Node> hits = technologySteps.get(TechnologyStep.TECHNOLOGY_STEP_ID, technologyStep.getTechnologyStepId());

        Transaction tx = graphDb.beginTx();
        try {
            if (hits.size() == 0) {
                techStepNode = graphDb.createNode();
                setTechStepProperties(techStepNode, technologyStep, json);
                addTechStepIndices(technologySteps, techStepNode, technologyStep);
            } else {
                techStepNode = hits.getSingle();
                String jsonString = (String) techStepNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);
                TechnologyStep oldTechStep = new Gson().fromJson(jsonString, TechnologyStep.class);
                removeTechStepIndices(technologySteps, techStepNode, oldTechStep);
                setTechStepProperties(techStepNode, technologyStep, json);
                addTechStepIndices(technologySteps, techStepNode, technologyStep);
            }
            tx.success();
        } finally {
            tx.finish();
        }

        return techStepNode;
    }
    
    /**
     * 
     * @param techStepNode
     * @param technologyStep
     * @param json 
     */
    public void                 setTechStepProperties(Node techStepNode, TechnologyStep technologyStep, String json) {
        
        techStepNode.setProperty(TechnologyStep.DESCRIPTION,             technologyStep.getDescription());
        techStepNode.setProperty(MachineResource.MACHINE_RESOURCE_ID,     technologyStep.getMachineResourceId());
        techStepNode.setProperty(Product.PRODUCT_ID,              technologyStep.getProductId());
        techStepNode.setProperty(TechnologyStep.PRODUCTION,              technologyStep.getProduction());
        techStepNode.setProperty(TechnologyStep.RESOURCE_REQUIRED,       technologyStep.getResourceRequired());
        techStepNode.setProperty(TechnologyStep.STEPS_REQUIRED,      
                new Gson().toJson(technologyStep.getStepsRequired(), Set.class));
        techStepNode.setProperty(TechnologyStep.STEP_STATUS,             technologyStep.getStepStatus());
        techStepNode.setProperty(TechnologyStep.TECH_STEP_NUMBER,        technologyStep.getTechStepNumber());
        techStepNode.setProperty(TechnologyStep.TECH_STEP_NUMBER,        technologyStep.getTechStepNumber());
        techStepNode.setProperty(TechnologyStep.TECHNOLOGY_STEP_ID,      technologyStep.getTechnologyStepId());
        techStepNode.setProperty(TechnologyStep.TECHNOLOGY_STEP_NAME,    technologyStep.getTechnologyStepName());
        techStepNode.setProperty(TechnologyStep.TECHNOLOGY_STEP_JSON,    json);
    }

    /**
     * 
     * @param technologySteps
     * @param techStepNode
     * @param technologyStep 
     */
    public void                 addTechStepIndices(Index<Node> technologySteps, Node techStepNode, TechnologyStep technologyStep) {
        technologySteps.add(techStepNode, TechnologyStep.TECHNOLOGY_STEP_ID, technologyStep.getTechnologyStepId());
        technologySteps.add(techStepNode, MachineResource.MACHINE_RESOURCE_ID, technologyStep.getMachineResourceId());
        technologySteps.add(techStepNode, Product.PRODUCT_ID, technologyStep.getProductId());
    }    
    
    /**
     * 
     * @param technologySteps
     * @param techStepNode
     * @param technologyStep 
     */
    public void                 removeTechStepIndices(Index<Node> technologySteps, Node techStepNode, TechnologyStep technologyStep) {
        technologySteps.remove(techStepNode, TechnologyStep.TECHNOLOGY_STEP_ID, technologyStep.getTechnologyStepId());
        technologySteps.remove(techStepNode, MachineResource.MACHINE_RESOURCE_ID, technologyStep.getMachineResourceId());
        technologySteps.remove(techStepNode, Product.PRODUCT_ID, technologyStep.getProductId());
    }

    /**
     * Creates relationships between technology steps. Relation name is an end
     * node (tech step) Id; technology step id property is a start node (technology
     * step).
     * 
     * @param graphDb
     * @param node
     * @param json 
     */
    public synchronized void    createTechRels(GraphDatabaseService graphDb, String json){
        
        TechnologyStep endTechStep = new Gson().fromJson(json, TechnologyStep.class);
        
        IndexManager nodeIndex = graphDb.index();
        IndexManager relIndex = graphDb.index();
        Index<Relationship> techRels = relIndex.forRelationships(TECH_PROCESS_RELS);
        Index<Node> nodes = nodeIndex.forNodes(TECHNOLOGY_STEPS);
        
        IndexHits<Node> startHits = nodes.get(TechnologyStep.TECHNOLOGY_STEP_ID, endTechStep.getTechnologyStepId());
        Node node;
        if(startHits.size() == 0){
            return;
        } else {
            node = startHits.getSingle();
        }
        
        Set<String> set = endTechStep.getStepsRequired();

        if (set.isEmpty()) {
            return;            
        }         
        
        for(String entry : set) {            
            IndexHits<Node> hits = nodes.get(TechnologyStep.TECHNOLOGY_STEP_ID, entry);            
            Node currentNode;
            String nodeJson;            
            if (hits.size() == 0) {
                continue;
            } else {                
                currentNode = hits.getSingle();

                if (currentNode == null || !currentNode.hasProperty(TechnologyStep.TECHNOLOGY_STEP_JSON)) {
                    continue;
                } else {
                    nodeJson = (String) currentNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);
                }
                TechnologyStep techStep = new Gson().fromJson(nodeJson, TechnologyStep.class);
                // Check if relationship is in the index and add it, if it is not there yet
                IndexHits<Relationship> relHits = techRels.get(TechnologyStep.TECHNOLOGY_STEP_ID,
                        techStep.getTechnologyStepId());
                if (relHits.size() == 0) {
                    Transaction tx = graphDb.beginTx();
                    try {
                        Relationship rel = currentNode.createRelationshipTo(node, RelTypes.T_PROCESS);
                        rel.setProperty(TechnologyStep.TECHNOLOGY_STEP_ID, techStep.getTechnologyStepId());
                        rel.setProperty(REL_NAME, endTechStep.getTechnologyStepId());
                        techRels.add(rel, REL_NAME, endTechStep.getTechnologyStepId());

                        tx.success();
                    } finally {
                        tx.finish();
                    }
                }
            }
        }
    }
    
}
