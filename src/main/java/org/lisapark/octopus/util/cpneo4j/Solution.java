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

import choco.Choco;
import choco.Options;
import choco.cp.model.CPModel;
import choco.cp.solver.CPSolver;
import choco.cp.solver.search.integer.valiterator.DecreasingDomain;
import choco.kernel.common.logging.ChocoLogging;
import choco.kernel.model.Model;
import choco.kernel.model.variables.integer.IntegerExpressionVariable;
import choco.kernel.model.variables.integer.IntegerVariable;
import choco.kernel.solver.ContradictionException;
import choco.kernel.solver.Solver;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alex
 */
public class Solution {
    
    static final Logger LOG = LoggerFactory.getLogger(Solution.class);
    
    public static final String MAX_UTILIZATION  = "MAX_UTILIZATION";
    public static final String MIN_COST         = "MIN_COST";
    public static final Integer FIXED           = 1;
    
    private static final String TOTAL_COST      = "TOTAL_COST";
    private static final String TOTAL_RESOURCES = "TOTAL_RESOURCES";
    
    private static final Integer HIGH_COST      = 1000000;
    private static final Integer HIGH_RESOURCES = 1000000;
    
    private GraphDatabaseService            graphDb;
    private String                          resultFieldName;
    
    private Map<String, IntegerVariable>    machineVars;
    private Map<String, IntegerVariable>    productVars;
    private Map<String, IntegerVariable>    productionVars;
    private Map<String, IntegerVariable>    techStepVars;
    
    private Map<String, Node>               machineMap;
    private Map<String, Node>               productMap;
    private Map<String, Node>               productionMap;
    
    private Integer                         highCost;
    private Integer                         highResources;
    
    private Multimap<ProductTechnologyStep, IntegerVariable>            techStepsByProductVars;
    private Multimap<ProductTechnologyStep, IntegerExpressionVariable>  techStepsByProductCostVars;
    private Multimap<String, IntegerExpressionVariable>                 techStepsByMachineVars;
    
    private String domainOption;
    private static final String NEO4J_URL = "http://localhost:7474/db/data";
    
    public static void main(String[] args){       

        CpNeo4jUtils utils = new CpNeo4jUtils();

        if (utils.getGraphDbService() == null) {            
            if (utils.checkDatabaseIsRunning(NEO4J_URL) == 200) {
                utils.setGraphDbService(utils.newServerInstance(NEO4J_URL));
            } else {
                LOG.error("Neo4j server is not available.");
                return;
            }
        }

        GraphDatabaseService _graphDb = utils.getGraphDbService();

        IndexManager index = _graphDb.index();
        Index<Node> solutions = index.forNodes(CpNeo4jUtils.SOLUTIONS);
        
        IndexHits<Node> hits = solutions.get(CpNeo4jUtils.SOLUTION_NAME, CpNeo4jUtils.ROOT_SOLUTION);
                 
        if(hits.size() == 0){
            return;
        }
        
        Node solutionNode = hits.getSingle();        
        
        Solution solution = new Solution("production", null, null, null);
        
        Map<String, Integer> list = solution.solve(solutionNode, 90000, MAX_UTILIZATION);
       
    }
    
    public Solution(String resultFieldName, Integer highCost, Integer highResources, String domainOption){
        this.machineVars    = Maps.newHashMap();
        this.productVars    = Maps.newHashMap();
        this.productionVars = Maps.newHashMap();
        this.techStepVars   = Maps.newHashMap();        
        
        this.resultFieldName   = resultFieldName;
        
        this.machineMap     = Maps.newHashMap();
        this.productMap     = Maps.newHashMap();
        this.productionMap  = Maps.newHashMap();
        
        this.techStepsByProductVars     = ArrayListMultimap.create();
        this.techStepsByMachineVars     = ArrayListMultimap.create();
        this.techStepsByProductCostVars = ArrayListMultimap.create();
        
        this.domainOption   = domainOption == null ? Options.V_BOUND : domainOption;        
        this.highCost       = highCost == null ? HIGH_COST : highCost;
        this.highResources  = highResources == null ? HIGH_RESOURCES : highResources;
    }
    
    /**
     * 
     * @param solution
     * @param timeLimit
     * @param optiOption
     * @return 
     */
    public Map<String, Integer> solve(Node solution, Integer timeLimit, String optiOption){
        
        Solver solver       = new CPSolver();         
        Model chocoModel    = new CPModel();        
        
        // Collecting constraint solution data
        //======================================================================
        Traverser productsAndMachines = getProductsAndMachinesTraverser(solution);
        
        // 1. Collect data about products and machines
        for(Node node : productsAndMachines.nodes()){
            String nodeType = (String) node.getSingleRelationship(
                    CpNeo4jUtils.RelTypes.SOLUTION, 
                    Direction.BOTH).getProperty(CpNeo4jUtils.NODE_TYPE);
            if(CpNeo4jUtils.PRODUCT.equalsIgnoreCase(nodeType)){
                addProductVar(node);
                addProductionVar(node);
            } else {
                addMachineVar(node);                
            }
        }
        
        // 2. Create all technology steps Integer Variables by traversing
        //    Product variables and populating Product lists at the same time
        Map<String, Node> productNodes = this.getProductMap();
        for (Entry<String, Node> entry : productNodes.entrySet()) {
            Node node = entry.getValue();
            Traverser prodTechSteps = getProductTechStepsTraverser(node);
            for (Node techStepNode : prodTechSteps.nodes()) {
                addTechnologyStepVar2ProductLists(techStepNode);
            }
        }
                        
        // 3. Collect data about technology steps for each machine
        Map<String, Node> machineNodes = this.getMachineMap();
        for (Entry<String, Node> entry : machineNodes.entrySet()) {
            Node node = entry.getValue();
            Traverser machineTechSteps = getMachineTechStepsTraverser(node);
            for (Node techStepNode : machineTechSteps.nodes()) {
                addTechnologyStepVar2MachineLists(techStepNode);
            }
        }
        
        // Add IntegerVariables and constraints to the model
        chocoModel = addDataUnitVariables(chocoModel);
        chocoModel = addProductVariables(chocoModel);
        chocoModel = addProductionVariables(chocoModel);
        chocoModel = addResourceVariables(chocoModel);
        chocoModel = addProductionConstraints(chocoModel);
        chocoModel = addResourceConstraints(chocoModel);              
        
        // 4. Set the goal
        IntegerExpressionVariable   goal;
        IntegerVariable             goalVar; 
        
        if(MIN_COST.equalsIgnoreCase(optiOption)){
            goal = minCost();
            goalVar = Choco.makeIntVar(TOTAL_COST, 0, getHighCost(), getDomainOption());
            chocoModel.addVariable(Options.V_OBJECTIVE, goal); 
        } else {
            goal = maxUtilization();
            goalVar = Choco.makeIntVar(TOTAL_RESOURCES, 0, getHighResources(), Options.V_OBJECTIVE);
            chocoModel.addVariable(Options.V_BOUND, goal); 
        }
             
        chocoModel.addConstraint(Choco.eq(goal, goalVar));
        
        LOG.info(chocoModel.pretty());
       
        // Specify Solver and perform calculations
        //======================================================================
        solver.read(chocoModel);

        try {
            solver.propagate();
        } catch (ContradictionException e) {
            LOG.error(e.getMessage());
        }

        solver.setValIntIterator(new DecreasingDomain());

        LOG.info("solver.setValIntIterator:");

        solver.setTimeLimit(timeLimit);
        ChocoLogging.toVerbose();

        if(MIN_COST.equalsIgnoreCase(optiOption)){
            solver.minimize(solver.getVar(goal), false);
        } else {
            solver.maximize(solver.getVar(goalVar), false);
        }        
        
        Map<String, Integer> map = Maps.newHashMap();
               
        if (solver.getNbSolutions() > 0) {
            Map<String, IntegerVariable> intVar = this.getTechStepVars();
            for (Entry<String, IntegerVariable> item : intVar.entrySet()) {
                IntegerVariable intVarEntry = item.getValue();
                if (intVarEntry != null && solver.getVar(intVarEntry) != null) {
                    Integer intValue = solver.getVar(intVarEntry).getVal();
                    String name = intVarEntry.getName();
                    map.put(name, intValue);
                }
            }
        }
        
        LOG.info(map.toString());        

        ChocoLogging.flushLogs();

        solver.getEnvironment().clear();

        return map;
    }
    
    private IntegerExpressionVariable   maxUtilization(){
        IntegerExpressionVariable maxUtil;
        
        int size = getMachineVars().size();
        IntegerVariable[] array = new IntegerVariable[size];
        
        array = getMachineVars().values().toArray(array);
        
        maxUtil = Choco.sum(array);
        
        return maxUtil;
    }
    
    private IntegerExpressionVariable   minCost(){
        IntegerExpressionVariable minCost;
        
        int size = getTechStepsByProductCostVars().size();
        IntegerVariable[] array = new IntegerVariable[size];        
        array = getTechStepsByProductCostVars().values().toArray(array);
        
        minCost = Choco.sum(array);
        
        return minCost;
    }
    
    private Traverser getProductsAndMachinesTraverser(final Node solution) {
        TraversalDescription td = Traversal.description()
                .breadthFirst()
                .relationships(CpNeo4jUtils.RelTypes.SOLUTION, Direction.INCOMING)
                .evaluator(Evaluators.excludeStartPosition());
        return td.traverse(solution);
    }    
    
    private Traverser getProductTechStepsTraverser(final Node product) {
        TraversalDescription td = Traversal.description()
                .breadthFirst()
                .relationships(CpNeo4jUtils.RelTypes.PRODUCT, Direction.INCOMING)
                .evaluator(Evaluators.excludeStartPosition());
        return td.traverse(product);
    }
    
    private Traverser getMachineTechStepsTraverser(final Node machine) {
        TraversalDescription td = Traversal.description()
                .breadthFirst()
                .relationships(CpNeo4jUtils.RelTypes.MACHINE, Direction.INCOMING)
                .evaluator(Evaluators.excludeStartPosition());
        return td.traverse(machine);
    }
    
    /**
     * 
     * @param id
     * @return 
     */
    public IntegerExpressionVariable[]                  getProductExpressionsAsArray(ProductTechnologyStep id){
        int size = getTechStepsByProductVars().get(id).size();
        IntegerExpressionVariable[] array = new IntegerExpressionVariable[size];
        array = getTechStepsByProductVars().get(id).toArray(array);
        return array;
    }
    
    /**
     * 
     * @param id
     * @return 
     */
    public IntegerExpressionVariable[]                  getMachineExpressionsAsArray(String id){
        int size = getTechStepsByMachineVars().get(id).size();
        IntegerExpressionVariable[] array = new IntegerExpressionVariable[size];
        array = getTechStepsByMachineVars().get(id).toArray(array);
        return array;
    }
    
    /**
     * 
     * @param product
     * @return 
     */
    public Solution                                     addProductVar(Node product){
        String productId = product.getProperty(Product.PRODUCT_ID).toString();
        
        getProductMap().put(productId, product);
        
        IntegerVariable productVar = Choco.makeIntVar(productId, 
                Integer.parseInt(product.getProperty(Product.PRODUCTION_LOW).toString()),
                Integer.parseInt(product.getProperty(Product.PRODUCTION_UPPER).toString()));
        
        getProductVars().put(productId, productVar);
        
        return this;
    }
    
    /**
     * 
     * @param product
     * @return 
     */
    public Solution                                     addProductionVar(Node product){
        String productId = product.getProperty(Product.PRODUCT_ID).toString();
        
        getProductionMap().put(productId, product);
        
        IntegerVariable productVar = Choco.makeIntVar(productId, 
                Integer.parseInt(product.getProperty(Product.PRODUCTION_VALUE).toString()),
                Integer.parseInt(product.getProperty(Product.PRODUCTION_VALUE).toString()));
        
        getProductionVars().put(productId, productVar);
        
        return this;
    }
    
    /**
     * 
     * @param machine
     * @return 
     */
    public Solution                                     addMachineVar(Node machine){
        
        String machineId = machine.getProperty(MachineResource.MACHINE_RESOURCE_ID).toString();
        
        getMachineMap().put(machineId, machine);
        
        IntegerVariable machineVar = Choco.makeIntVar(machineId, 
                Integer.parseInt(machine.getProperty(MachineResource.LOW_BOUND).toString()),
                Integer.parseInt(machine.getProperty(MachineResource.UPPER_BOUND).toString()));
        
        getMachineVars().put(machineId, machineVar);
        
        return this;
    }
    
    /**
     * 
     * @param techStep
     * @return 
     */
    public Solution                                     addTechnologyStepVar2ProductLists(Node techStepNode){
      
        String json                 = (String) techStepNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);
        TechnologyStep techStep     = new Gson().fromJson(json, TechnologyStep.class);

        String techStepId           = techStep.getTechnologyStepId();
        String productId            = techStep.getProductId();
        Integer techStepNumber      = techStep.getTechStepNumber();
        ProductTechnologyStep num   = new ProductTechnologyStep(productId, techStepNumber);
        String machineId            = techStep.getMachineResourceId();
        
        String machineJson          = (String) getMachineById(machineId).getProperty(MachineResource.MACHINE_JSON);
        MachineResource machine     = new Gson().fromJson(machineJson, MachineResource.class);
        String productJson          = (String) getProductById(productId).getProperty(Product.PRODUCT_JSON);
        Product product             = new Gson().fromJson(productJson, Product.class);
        
        Integer techStepStatus      = techStep.getStepStatus();
        Integer resourceUnitCost    = machine.getResourceUnitCost();        
        Integer resurcePerUnit      = techStep.getResourceRequired();

        IntegerVariable stepP;
        Integer prodLow;
        Integer prodUpper;
        if (techStepStatus == FIXED) {
            Integer fixedValue = techStep.getProduction();
            prodLow     = fixedValue;
            prodUpper   = fixedValue;
        } else {
            prodLow     = 0;
            prodUpper   = product.getProductionUpperBound();            
        }
        
        stepP = Choco.makeIntVar(techStepId, prodLow, prodUpper);
        getTechStepVars().put(techStepId, stepP);
            
        IntegerExpressionVariable cost = Choco.mult(stepP, ((int)resourceUnitCost * (int)resurcePerUnit));
        
        getTechStepsByProductCostVars().get(num).add(cost);
        getTechStepsByProductVars().get(num).add(stepP); 

        return this;
    }
    
    public Solution                                     addTechnologyStepVar2MachineLists(Node techStepNode){
      
        String json                 = (String) techStepNode.getProperty(TechnologyStep.TECHNOLOGY_STEP_JSON);
        TechnologyStep techStep     = new Gson().fromJson(json, TechnologyStep.class);

        String techStepId           = techStep.getTechnologyStepId();
        String machineId            = techStep.getMachineResourceId();
        Integer resurcePerUnit      = techStep.getResourceRequired();
        
        IntegerVariable stepM       = this.getTechStepVars().get(techStepId);         
        IntegerExpressionVariable resource    = Choco.mult(stepM, (int)resurcePerUnit);
        
        getTechStepsByMachineVars().get(machineId).add(resource);

        return this;
    }
    
    // Properties
    //==========================================================================

    /**
     * @return the graphDb
     */
    public GraphDatabaseService                         getGraphDb() {
        return graphDb;
    }

    /**
     * @param graphDb the graphDb to set
     */
    public void                                         setGraphDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * @return the solutionName
     */
    public String                                       getResultFieldName() {
        return resultFieldName;
    }

    /**
     * @param solutionName the solutionName to set
     */
    public void                                         setResultFieldName(String resultFieldName) {
        this.resultFieldName = resultFieldName;
    }

    /**
     * @return the machines
     */
    public Map<String, IntegerVariable>                 getMachineVars() {
        return machineVars;
    }

    /**
     * @param machines the machines to set
     */
    public void                                         setMachineVars(Map<String, IntegerVariable> machines) {
        this.machineVars = machines;
    }

    /**
     * @return the products
     */
    public Map<String, IntegerVariable>                 getProductVars() {
        return productVars;
    }

    /**
     * @param products the products to set
     */
    public void                                         setProductVars(Map<String, IntegerVariable> products) {
        this.productVars = products;
    }

    /**
     * @return the productionVars
     */
    public Map<String, IntegerVariable>                 getProductionVars() {
        return productionVars;
    }

    /**
     * @param productionVars the productionVars to set
     */
    public void                                         setProductionVars(Map<String, IntegerVariable> productionVars) {
        this.productionVars = productionVars;
    }
    
    /**
     * @return the machineMap
     */
    public Map<String, Node>                            getMachineMap() {
        return machineMap;
    }

    /**
     * @param machineMap the machineMap to set
     */
    public void                                         setMachineMap(Map<String, Node> machineMap) {
        this.machineMap = machineMap;
    }

    /**
     * @return the productMap
     */
    public Map<String, Node>                            getProductMap() {
        return productMap;
    }

    /**
     * @param productMap the productMap to set
     */
    public void                                         setProductMap(Map<String, Node> productMap) {
        this.productMap = productMap;
    }

    /**
     * @return the techStepsByProduct
     */
    public Multimap<ProductTechnologyStep, IntegerVariable> getTechStepsByProductVars() {
        return techStepsByProductVars;
    }

    /**
     * @param techStepsByProduct the techStepsByProduct to set
     */
    public void                                         setTechStepsByProduct(
            Multimap<ProductTechnologyStep, IntegerVariable> techStepsByProduct) {
        this.techStepsByProductVars = techStepsByProduct;
    }

    /**
     * @return the techStepsByMachine
     */
    public Multimap<String, IntegerExpressionVariable>  getTechStepsByMachineVars() {
        return techStepsByMachineVars;
    }

    /**
     * @param techStepsByMachine the techStepsByMachine to set
     */
    public void                                         setTechStepsByMachine(
            Multimap<String, IntegerExpressionVariable> techStepsByMachine) {
        this.techStepsByMachineVars = techStepsByMachine;
    }

    /**
     * @return the domainOption
     */
    public String                                       getDomainOption() {
        return domainOption;
    }
    
    /**
     * 
     * @param id
     * @return 
     */
    public Node                                         getProductById(String id){
        return this.productMap.get(id);
    }
    
    /**
     * 
     * @param id
     * @return 
     */
    public Node                                         getMachineById(String id){
        return this.machineMap.get(id);
    }

    /**
     * @return the techStepsByProductCostVars
     */
    public Multimap<ProductTechnologyStep, IntegerExpressionVariable>  getTechStepsByProductCostVars() {
        return techStepsByProductCostVars;
    }

    /**
     * @param techStepsByProductCostVars the techStepsByProductCostVars to set
     */
    public void                                         setTechStepsByProductCostVars(
            Multimap<ProductTechnologyStep, IntegerExpressionVariable> techStepsByProductCostVars) {
        this.techStepsByProductCostVars = techStepsByProductCostVars;
    }

    /**
     * @return the highCost
     */
    public Integer                                      getHighCost() {
        return highCost;
    }

    /**
     * @param highCost the highCost to set
     */
    public void                                         setHighCost(Integer highCost) {
        this.highCost = highCost;
    }

    /**
     * @return the highResources
     */
    public Integer                                      getHighResources() {
        return highResources;
    }

    /**
     * @param highResources the highResources to set
     */
    public void                                         setHighResources(Integer highResources) {
        this.highResources = highResources;
    }

    /**
     * @return the techStepMap
     */
    public Map<String, IntegerVariable>                 getTechStepVars() {
        return techStepVars;
    }

    /**
     * @param techStepMap the techStepMap to set
     */
    public void                                         setTechStepVars(Map<String, IntegerVariable> techStepVars) {
        this.techStepVars = techStepVars;
    }

    // Helper methods to specify Choco Model
    //==========================================================================
    
    /**
     * 
     * @param chocoModel
     * @return 
     */
    private Model   addDataUnitVariables(Model chocoModel) {
        Collection<IntegerVariable> collection = this.getTechStepsByProductVars().values();
        for(IntegerVariable var : collection){
            chocoModel.addVariable(var);
        }
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    private Model   addProductVariables(Model chocoModel) {
        Set<Entry<String, IntegerVariable>> prodSet = getProductVars().entrySet();
        for (Iterator<Entry<String, IntegerVariable>> it = prodSet.iterator(); it.hasNext();) {
            Entry<String, IntegerVariable>  entry = it.next();
            chocoModel.addVariable(entry.getValue());
        }
        
        return chocoModel;
    }

    private Model addProductionVariables(Model chocoModel) {
        Set<Entry<String, IntegerVariable>> prodctionSet = getProductionVars().entrySet();
        for (Iterator<Entry<String, IntegerVariable>> it = prodctionSet.iterator(); it.hasNext();) {
            Entry<String, IntegerVariable>  entry = it.next();
            chocoModel.addVariable(entry.getValue());
        }
        
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    private Model   addResourceVariables(Model chocoModel) {
        Set<Entry<String, IntegerVariable>> machineSet = getMachineVars().entrySet();
        for (Iterator<Entry<String, IntegerVariable>> it = machineSet.iterator(); it.hasNext();) {
            Entry<String, IntegerVariable>  entry = it.next();
            chocoModel.addVariable(entry.getValue());
        }
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    private Model   addProductionConstraints(Model chocoModel) {
        Map<ProductTechnologyStep, Collection<IntegerVariable>> prodSet = this.getTechStepsByProductVars().asMap();
        for (Entry<ProductTechnologyStep, Collection<IntegerVariable>> entry : prodSet.entrySet()) {
            String step = entry.getKey().getProductId();
            IntegerVariable product = this.getProductionVars().get(step);
            chocoModel.addConstraint(Choco.eq(product, 
                    Choco.sum(getProductExpressionsAsArray(entry.getKey()))));
        }
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    private Model   addResourceConstraints(Model chocoModel) {
        Set<Entry<String, IntegerVariable>> machineSet = getMachineVars().entrySet();
        for (Iterator<Entry<String, IntegerVariable>> it = machineSet.iterator(); it.hasNext();) {
            Entry<String, IntegerVariable>  entry = it.next();
            chocoModel.addConstraint(Choco.eq(entry.getValue(), 
                    Choco.sum(getMachineExpressionsAsArray(entry.getKey()))));
        }
        return chocoModel;
    }

    /**
     * @return the productionMap
     */
    public Map<String, Node> getProductionMap() {
        return productionMap;
    }

    /**
     * @param productionMap the productionMap to set
     */
    public void setProductionMap(Map<String, Node> productionMap) {
        this.productionMap = productionMap;
    }

    
}
