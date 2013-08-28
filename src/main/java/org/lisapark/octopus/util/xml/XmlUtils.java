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
package org.lisapark.octopus.util.xml;

import choco.Choco;
import choco.Options;
import choco.cp.model.CPModel;
import choco.cp.solver.CPSolver;
import choco.cp.solver.search.integer.valiterator.DecreasingDomain;
import choco.kernel.model.Model;
import choco.kernel.model.variables.integer.IntegerVariable;
import choco.kernel.solver.Solver;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;
import com.googlecode.sardine.impl.SardineException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.IOUtils;
import org.lisapark.octopus.util.Pair;
import org.openide.util.Exceptions;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class XmlUtils {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(XmlUtils.class.getName());
    
    public static final String PROD             = "prod";
    public static final String PROCESS_STEP     = "step";
    public static final String PROCESSOR        = "machine";
    public static final String PROCESS_COST     = "cost";
    public static final String UNIT_VALUE       = "unitvalue";
    public static final String PROD_VALUE       = "value";
    public static final String LOW_BOUND        = "lowbound";
    public static final String UPPER_BOUND      = "upperbound";
    public static final String PLAN_VALUE       = "plan";
    public static final String RESOURCE         = "resource";
    public static final String RESOURCE_VALUE   = "value";
    public static final String FIXED            = "fixed";
    
    
    public static final String TOTAL_COST       = "totalcost";    
    public static final String TOTAL_RESOURCES  = "totalresources";
    
    public static final Integer LOW_VALUE   = 0;
    public static final Integer HIGH_VALUE  = 2000000;
    public static final Integer HIGH_COST   = 10000000; 
    
    public static final String MIN_COST        = "MIN";
    public static final String MAX_RESOURCE    = "MAX";
    
    // Standard key names
    public static final String KEY_PROD             = "PROD";
    public static final String KEY_PROCESS_STEP     = "PROCESS_STEP";
    public static final String KEY_PROCESSOR        = "PROCESSOR";
    public static final String KEY_PROCESS_COST     = "PROCESS_COST";
    public static final String KEY_UNIT_VALUE       = "UNIT_VALUE";
    public static final String KEY_PROD_VALUE       = "PROD_VALUE";
    public static final String KEY_LOW_BOUND        = "LOW_BOUND";
    public static final String KEY_UPPER_BOUND      = "UPPER_BOUND";
    public static final String KEY_PLAN_VALUE       = "PLAN_VALUE";
    public static final String KEY_RESOURCE         = "RESOURCE";
    public static final String KEY_RESOURCE_VALUE   = "RESOURCE_VALUE";
    public static final String KEY_FIXED            = "FIXED";
    
    
    public static final String KEY_TOTAL_COST       = "TOTAL_COST";    
    public static final String KEY_TOTAL_RESOURCES  = "TOTAL_RESOURCES";
    
    // Class variables
    private Map<String, String> attrNames;
    private String optiMode;
    
    public static void main(String[] args) 
            throws ParserConfigurationException, SAXException, IOException{
        
        String dictionary = 
                "PROD=PRODUCT_ID,"
            + "PROCESS_STEP=TECHNOLOGY_STEP_NUMBER,"
            + "PROCESSOR=MACHINE_ID,"
            + "PROCESS_COST=COST_PER_UNIT,"            
            + "UNIT_VALUE=MACHINE_RESOURCE_PER_PRODUCT_UNIT,"
            + "PROD_VALUE=PROD_VALUE,"
            + "LOW_BOUND=LOWBOUND,"
            + "UPPER_BOUND=UPPERBOUND,"
            + "PLAN_VALUE=PLAN_VALUE,"
            + "RESOURCE=MACHINE_ID,"
            + "RESOURCE_VALUE=RESOURCE_VALUE,"
            + "FIXED=FIXED";
        
        XmlUtils grid = new XmlUtils(dictionary);
        
        String dataFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Data.xml";
        String planFile = "http://173.72.110.131:8080/WebDavServer/iPlast/Plan.xml";
        String recFile  = "http://173.72.110.131:8080/WebDavServer/iPlast/Resources.xml";
        
        // Get all xml files
        Sardine sardine = SardineFactory.begin("", "");

        InputStream isData = sardine.get(dataFile);
        String xmlData = IOUtils.toString(isData);
        
        InputStream isPlan = sardine.get(planFile);
        String xmlPlan = IOUtils.toString(isPlan);
        
        InputStream isRec = sardine.get(recFile);
        String xmlRec = IOUtils.toString(isRec);
        
        // Conver string xml files to java collections
        List<Map<String, Object>> data                  = grid.getXmlStringAsList(xmlData);
              
        Map<String, Map<String, Object>> planDataMap    = grid.getXmlStringAsMap(xmlPlan, 
                grid.getAttrNames().get(KEY_PROD));
                
        Map<String, Map<String, Object>> machineDataMap = grid.getXmlStringAsMap(xmlRec, 
                grid.getAttrNames().get(KEY_RESOURCE)); 
        
        // Set optimization mode
        //======================================================================
        grid.setOptiMode(MIN_COST);
//        grid.setOptiMode(MAX_RESOURCE);

        List<Map<String, Object>> dataList = grid.performSimulation(data, planDataMap, machineDataMap, 0, 0, 0);
             
        String outputString = grid.formatOutput(dataList);
        
        grid.saveStringAsFile(outputString, dataFile);
        
        System.out.println(outputString);
    }
    
    public XmlUtils(String dictionary){
        // Craete default dictionary
        attrNames = Maps.newHashMap();
        
        attrNames.put(KEY_PROD, PROD);
        attrNames.put(KEY_PROCESS_STEP, PROCESS_STEP);
        attrNames.put(KEY_PROCESSOR, PROCESSOR);
        attrNames.put(KEY_PROCESS_COST, PROCESS_COST);
        attrNames.put(KEY_UNIT_VALUE, UNIT_VALUE);
        attrNames.put(KEY_PROD_VALUE, PROD_VALUE);
        attrNames.put(KEY_LOW_BOUND, LOW_BOUND);
        attrNames.put(KEY_UPPER_BOUND, UPPER_BOUND);
        attrNames.put(KEY_PLAN_VALUE, PLAN_VALUE);
        attrNames.put(KEY_RESOURCE, RESOURCE);
        attrNames.put(KEY_RESOURCE_VALUE, RESOURCE_VALUE);
        attrNames.put(KEY_FIXED, FIXED);
       
        if(dictionary == null || dictionary.isEmpty()) {
            return;
        }
        
        String[] terms = dictionary.split(",");
        
        for(int i = 0; i < terms.length; i++){
            String[] pair = terms[i].split("=");
            String key = pair[0].trim();
            if(attrNames.containsKey(key)){
                attrNames.put(key, pair[1].trim());
            }            
        }        
    }
    
    /**
     * 
     * @param itemMap
     * @return
     * @throws NumberFormatException
     * @throws DOMException 
     */
    private Map<String, Object> getGridItemMap(NamedNodeMap itemMap) 
            throws NumberFormatException, DOMException {
        
        Map<String, Object> dataItem = Maps.newHashMap();
        Map<String, String> map = getAttrNames();
        
        for (int k = 0; k < itemMap.getLength(); k++) {
            Node attr = itemMap.item(k);
            String attrName = attr.getNodeName();
            
            if (map.get(KEY_FIXED).equalsIgnoreCase(attrName)
                    || map.get(KEY_LOW_BOUND).equalsIgnoreCase(attrName)
                    || map.get(KEY_UPPER_BOUND).equalsIgnoreCase(attrName)
                    || map.get(KEY_PLAN_VALUE).equalsIgnoreCase(attrName)
                    || map.get(KEY_PROCESS_COST).equalsIgnoreCase(attrName)
                    || map.get(KEY_PROD_VALUE).equalsIgnoreCase(attrName)
                    || map.get(KEY_RESOURCE_VALUE).equalsIgnoreCase(attrName)
                    || map.get(KEY_PROCESS_STEP).equalsIgnoreCase(attrName)
                    || map.get(KEY_UNIT_VALUE).equalsIgnoreCase(attrName)) {
                String attrString = attr.getTextContent();
                int value = attrString.isEmpty() ? 0 : Integer.parseInt(attrString);
                dataItem.put(attrName, value);
            } else {
                String attrString = attr.getTextContent();
                dataItem.put(attrName, attrString);
            }
        }
        
        return dataItem;
    }

    public synchronized static String clean(String string){
        StringBuilder cleanStr = new StringBuilder();
        
        Character lookFor = '<';
        
        for(Character ch : string.toCharArray()){
            if(ch == lookFor){
                lookFor = lookFor == '<' ? '>' : '<'; 
                cleanStr.append(ch);
            } else if(lookFor == '>'){
                cleanStr.append(ch);
            }            
        }
        
        return cleanStr.toString();
    }
                
    /**
     * 
     * @param data
     * @param planDataMap
     * @return 
     */
    public Map<String, Integer> getProdMap(List<Map<String, Object>> data, 
            Map<String, Map<String,Object>> planDataMap){

        Map<String, Integer> prodMap = Maps.newHashMap();
        
        for (Map<String, Object> item : data) {
            String prodName = (String) item.get(getAttrNames().get(KEY_PROD));
            Object object = planDataMap.get(prodName).get(getAttrNames().get(KEY_PLAN_VALUE));
            int prodPlan = convert2Int(object);
            prodMap.put(prodName + "_" 
                    + item.get(getAttrNames().get(KEY_PROCESS_STEP)), prodPlan);
        }
        
        return prodMap;
    }
  
    /**
     * 
     * @param xmlString
     * @return 
     */
    public Map<String, Map<String, Object>> getMachineMap(String xmlString){
        Map<String, Map<String, Object>> machineMap = Maps.newHashMap();
        
        return machineMap;
    }
    
    /**
     * 
     * @param prodDataMap
     * @return 
     */
    public Map<String, List<String>> createProdMap(Map<String, Integer> prodDataMap) {
        // Map to collect variables for prducts
        Map<String, List<String>> prodVarMap = Maps.newHashMap();
        for(Entry<String, Integer> item : prodDataMap.entrySet()){
            List<String> varList = Lists.newArrayList();
            prodVarMap.put(item.getKey(), varList);
        }
        return prodVarMap;
    }

    /**
     * 
     * @param machineDataMap
     * @return 
     */
    public Map<String, List<String>> createMachineMap(Map<String, Map<String, Object>> machineDataMap) {
        // Map to collect Machine variables
        Map<String, List<String>> machineVarMap = Maps.newHashMap();
        for(Entry<String, Map<String, Object>> item : machineDataMap.entrySet()){
            List<String> varList = Lists.newArrayList();
            machineVarMap.put(item.getKey(), varList);
        }
        return machineVarMap;
    }
    
    /**
     * 
     * @param varMap
     * @param unitValueMap
     * @param intVarMap
     * @return 
     */
    public Map<String, Pair<int[], IntegerVariable[]>> createVarMapArray(
            Map<String, List<String>> varMap, 
            Map<String, Integer> unitValueMap,
            Map<String, IntegerVariable> intVarMap) {
        
        Map<String, Pair<int[], IntegerVariable[]>> varMapArray = Maps.newHashMap();
        
        for (Entry entry : varMap.entrySet()) {
            String name     = (String) entry.getKey();
            List varList    = (List)varMap.get(name);
            int uvaSize     = (int)varList.size();
            
            int[] unitValueArray = new int[uvaSize];
            
            IntegerVariable[] intVariables = new IntegerVariable[uvaSize];
            
            for(int i = 0; i < uvaSize; i++){
                String varName = (String) varList.get(i);
                unitValueArray[i]   = unitValueMap.get(varName);
                intVariables[i]     = intVarMap.get(varName);                
            } 
            Pair pair = Pair.newInstance(unitValueArray, intVariables);
            varMapArray.put(name, pair);
        }
        
        return varMapArray;
    }

    /**
     * 
     * @param costMap
     * @param unitValueMap
     * @return 
     */
    public Pair<int[], IntegerVariable[]> mergeVarMapArray(
            Map<String, Integer> costMap, Map<String, IntegerVariable> unitValueMap) {
       
        int arraySize = costMap.size();
        
        int[] costs               = new int[arraySize];
        IntegerVariable[] intVars   = new IntegerVariable[arraySize];
        
        int i = 0;
        for (Entry entry : costMap.entrySet()) {
            String name = (String) entry.getKey();
            costs[i]  = (Integer)entry.getValue();
            intVars[i]  = unitValueMap.get(name);            
            i++;
        }
        
        Pair<int[], IntegerVariable[]> pair = Pair.newInstance(costs, intVars);
        
        return pair;
    }

    /**
     * 
     * @param data
     * @param intVarMap
     * @param solver
     * @return 
     */
    public List<Map<String, Object>> updateData(
            List<Map<String, Object>> data, 
            Map<String, IntegerVariable> intVarMap,
            Solver solver) {
      
        if (solver.getNbSolutions() > 0) {
            for (Map<String, Object> item : data) {
                String name = getName(item);
                IntegerVariable intVar = intVarMap.get(name);
                item.put(getAttrNames().get(KEY_PROD_VALUE), solver.getVar(intVar).getVal());
            }
        }
        return data;
    }

    /**
     * 
     * @param map
     * @return 
     */
    public String getName(Map<String, Object> map) {
        String name = (String) map.get(getAttrNames().get(KEY_PROD)) + "_"
                + (Integer) map.get(getAttrNames().get(KEY_PROCESS_STEP)) + "_"
                + (String) map.get(getAttrNames().get(KEY_PROCESSOR));
        return name;
    }

    // Perform Simulation process to find the optimal solution
    //==========================================================================
    public synchronized List<Map<String, Object>> performSimulation(List<Map<String, Object>> dataList, 
            Map<String, Map<String, Object>> planDataMap, 
            Map<String, Map<String, Object>> machineDataMap,
            int lowValue,
            int highValue,
            int highCost) {
        // Setting up upper boundries for plan variables
        if(highValue == 0){
            highValue = getUpperBound(planDataMap);
        }
        
        if(highCost == 0){
            highCost = 10000000;
        }
        
        Map<String, Integer> prodDataMap        = getProdMap(dataList, planDataMap);
        Map<String, List<String>> prodVarMap    = createProdMap(prodDataMap);
        Map<String, List<String>> machineVarMap = createMachineMap(machineDataMap);
        // Make a map to collect profit and unit values
        Map<String, Integer> costMap            = Maps.newHashMap();
        Map<String, Integer> unitValueMap       = Maps.newHashMap();
        
        // Now we are ready to make a model
        //======================================================================
        Model model = new CPModel();
        // Create variable for all products and machines.
        // This method also creates constraints for the fixed plan values.
        Map<String, IntegerVariable> intVarMap = 
                createProductAndProcessorVariables(dataList, prodVarMap, machineVarMap, 
                costMap, unitValueMap, lowValue, highValue, model);
        
        logger.log(Level.INFO, "intVarMap: {0}", intVarMap);
        
        // Converting collections to arrays and IntegerVariables
        //======================================================================
        // Creating Goal criteria variable
        IntegerVariable goal;
        if(getOptiMode() == null ? MIN_COST == null : getOptiMode().equals(MIN_COST)){
            goal = Choco.makeIntVar(getAttrNames().get(KEY_TOTAL_COST), 1, highCost, Options.V_BOUND);
        } else {
            // Getting total resources
            int totalResource = getTotalResource(machineDataMap);
            goal = Choco.makeIntVar(getAttrNames().get(KEY_TOTAL_RESOURCES), 1, totalResource * 2, Options.V_BOUND);
        }
        // Machines
        Map<String, Pair<int[], IntegerVariable[]>> machineVarMapArray = 
                createVarMapArray(machineVarMap, unitValueMap, intVarMap);
        
        logger.log(Level.INFO, "machineVarMapArray: {0}", machineVarMapArray);
        
        // Products
        Map<String, Pair<int[], IntegerVariable[]>> prodVarMapArray = 
                createVarMapArray(prodVarMap, unitValueMap, intVarMap);
        
        logger.log(Level.INFO, "prodVarMapArray: {0}", prodVarMapArray);
        
        // create profits and unitValues  arrays
        Pair<int[], IntegerVariable[]> varMapArray =
                mergeVarMapArray(costMap, intVarMap);
        // Add product constraints to the model
        addProductConstraints(prodVarMapArray, planDataMap, model);
        
        logger.log(Level.INFO, "addProductConstraints:");
        
        // Add mschine constraints to the model
        IntegerVariable[] intVarResources = addProcessorConstraints(machineVarMapArray, machineDataMap, model);
        
        logger.log(Level.INFO, "addProcessorConstraints:");
        
        // Add cost criteria constrains
        int[] values =  varMapArray.getFirst();
        IntegerVariable[] intVars = varMapArray.getSecond();
        
        if(getOptiMode() == null ? MIN_COST == null : getOptiMode().equals(MIN_COST)){
            model.addConstraint(Choco.geq(Choco.scalar(values, intVars), goal));
        } else {
            model.addConstraint(Choco.leq(Choco.sum(intVarResources), goal));
        }
        
        logger.log(Level.INFO, "model.addConstraint:");
        
        Solver solver = new CPSolver();
        
        logger.log(Level.INFO, "Solver solver:");
        
        solver.read(model);
        
        logger.log(Level.INFO, "solver.read:");
        
        solver.setValIntIterator(new DecreasingDomain());
        
        logger.log(Level.INFO, "solver.setValIntIterator:");
        
        solver.setTimeLimit(30000);
        // Plan should minimize production cost
        if(getOptiMode() == null ? MIN_COST == null : getOptiMode().equals(MIN_COST)){
            solver.minimize(solver.getVar(goal), false);
            logger.log(Level.INFO, "Solutions MIN: {0}", solver.getNbSolutions());
        } else {
        // OR plan should maximize use of equipment
            solver.maximize(solver.getVar(goal), true);
            logger.log(Level.INFO, "Solutions MAX: {0}", solver.getNbSolutions());
        }
        
        logger.log(Level.INFO, "solver.minimize:");
        
        dataList = updateData(dataList, intVarMap, solver);
        
//        logger.log(Level.INFO, "Solutions: {0}", solver.getNbSolutions());
        logger.log(Level.INFO, "Goal value: {0}", solver.getOptimumValue());
        logger.log(Level.INFO, "Constraints value: {0}", solver.getModel().constraintsToString());
        logger.log(Level.INFO, "Solution value: {0}", solver.getModel().solutionToString());
        
        solver.getEnvironment().clear();
        
        return dataList;
    }
    
    
    private int getTotalResource(Map<String, Map<String, Object>> machineDataMap) {
        int totalResource = 0;
        
        for(Entry entry : machineDataMap.entrySet()){
            String name = getAttrNames().get(KEY_RESOURCE_VALUE);
            Map<String, Object> map = (Map<String, Object>) entry.getValue();
            int resourceValue = (Integer)map.get(name);
            totalResource=+ resourceValue;
        }        
        return totalResource;
    }
   
    /**
     * 
     * @param machineVarMapArray
     * @param machineDataMap
     * @param model 
     */
    public synchronized IntegerVariable[] addProcessorConstraints(
            Map<String, Pair<int[], IntegerVariable[]>> procVarMapArray,
            Map<String, Map<String, Object>> procDataMap,
            Model model) {
        
        
        IntegerVariable[] resourceIntVar = new IntegerVariable[procDataMap.size()];
        // Add machines constraints to the model
        int i = 0;
        for(Entry entry : procVarMapArray.entrySet()){
            String name     = (String) entry.getKey();
            int[] values    = (int[]) ((Pair)entry.getValue()).getFirst();
            IntegerVariable[] intVars = (IntegerVariable[]) ((Pair)entry.getValue()).getSecond();
            
            if(getOptiMode() == null ? MIN_COST == null : getOptiMode().equals(MIN_COST)){
//                resourceIntVar = null;
                minCostProcConstraints(procDataMap, name, model, values, intVars);
            } 
//            else {
                resourceIntVar[i] = maxResoiurceProcConstraints(procDataMap, name, model, values, intVars);
                i++;
//            }            
        }        
        return resourceIntVar;
    }

    private void minCostProcConstraints(Map<String, Map<String, Object>> procDataMap, 
            String name, Model model, int[] values, IntegerVariable[] intVars) {
       
        int resource    = (Integer)procDataMap.get(name).get(getAttrNames().get(KEY_RESOURCE_VALUE));
        int lowB        = (Integer)procDataMap.get(name).get(getAttrNames().get(KEY_LOW_BOUND));
        
        int uppB = (Integer)procDataMap.get(name)
                .get(getAttrNames().get(KEY_UPPER_BOUND));
        if(uppB == 0){        
            uppB = (int) (resource * 2);
        }  
        
        IntegerVariable intVar = Choco.makeIntVar(name, lowB, uppB);
        model.addConstraint(Choco.eq(Choco.scalar(values, intVars), intVar));
    }
    
    private IntegerVariable maxResoiurceProcConstraints(Map<String, Map<String, Object>> procDataMap, 
            String name, Model model, int[] values, IntegerVariable[] intVars) {
        
        int resource    = (Integer)procDataMap.get(name).get(getAttrNames().get(KEY_RESOURCE_VALUE));
        int lowB        = (Integer)procDataMap.get(name).get(getAttrNames().get(KEY_LOW_BOUND));
        
        int uppB = resource;
        
        IntegerVariable intVar = Choco.makeIntVar(name, lowB, uppB);
        model.addConstraint(Choco.eq(Choco.scalar(values, intVars), intVar));
        
        return intVar;
    }
   
    
    /**
     * 
     * @param prodVarMapArray
     * @param planDataMap
     * @param model 
     */
    public synchronized void addProductConstraints(
            Map<String, Pair<int[], IntegerVariable[]>> prodVarMapArray, 
            Map<String, Map<String, Object>> planDataMap,
            Model model) {
        // Add products constraints to the model
        for(Entry entry : prodVarMapArray.entrySet()){
            String name     = (String) entry.getKey();
            int[] values    = (int[]) ((Pair)entry.getValue()).getFirst();
            IntegerVariable[] intVars = (IntegerVariable[]) ((Pair)entry.getValue()).getSecond();

            // Remove step number from product name
            String prodName = name.split("_")[0];
            
            if(getOptiMode() == null ? MIN_COST == null : getOptiMode().equals(MIN_COST)){
                minCostProductConstraints(planDataMap, prodName, name, model, values, intVars);
            } else {
                maxResourseProductConstraints(planDataMap, prodName, name, model, values, intVars);
            }
        }        
    }
    
    private void minCostProductConstraints(Map<String, Map<String, Object>> planDataMap, 
            String prodName, String name, Model model, int[] values, IntegerVariable[] intVars) {
        
        int plan = (Integer) planDataMap.get(prodName).get(getAttrNames().get(KEY_PLAN_VALUE));

        int lowB = (Integer) planDataMap.get(prodName).get(getAttrNames().get(KEY_LOW_BOUND));
        if (lowB == 0) {
            lowB = (int) (plan * 1);
        }
        int uppB = (Integer) planDataMap.get(prodName).get(getAttrNames().get(KEY_UPPER_BOUND));
        if (uppB == 0) {
            uppB = (int) (plan * 1.4);
        }
        IntegerVariable intVar = Choco.makeIntVar(name, lowB, uppB, Options.V_BOUND);
        model.addConstraint(Choco.eq(Choco.scalar(values, intVars), intVar));
        
    }

    private void maxResourseProductConstraints(Map<String, Map<String, Object>> planDataMap,
            String prodName, String name, Model model, int[] values, IntegerVariable[] intVars) {

        int plan = (Integer)planDataMap.get(prodName).get(getAttrNames().get(KEY_PLAN_VALUE));
        
        int lowB = (Integer)planDataMap.get(prodName).get(getAttrNames().get(KEY_LOW_BOUND));
        
        int uppB = (Integer)planDataMap.get(prodName).get(getAttrNames().get(KEY_UPPER_BOUND));
        if(uppB == 0){        
            uppB = (int) (plan * 2);
        }
        IntegerVariable intVar = Choco.makeIntVar(name, lowB, uppB, Options.V_BOUND);
        model.addConstraint(Choco.eq(Choco.scalar(values, intVars), intVar));
    }

    /**
     * 
     * @param data
     * @param prodVarMap
     * @param machineVarMap
     * @param costMap
     * @param unitValueMap
     * @param intVarMap
     * @param model 
     */
    public synchronized Map<String, IntegerVariable> createProductAndProcessorVariables(List<Map<String, Object>> data, 
            Map<String, List<String>> prodVarMap, 
            Map<String, List<String>> procVarMap, 
            Map<String, Integer> costMap, 
            Map<String, Integer> unitValueMap,
            int lowValue,
            int highValue,
            Model model) {
        
        Map<String, IntegerVariable> intVarMap  = Maps.newHashMap();
        // Collect variables for products and machines
        for(Map<String, Object> map : data){
            // Use only those data that presented in Product and Machine Lists
            String prodName = (String)map.get(getAttrNames().get(KEY_PROD));
            Integer stepName = (Integer)map.get(getAttrNames().get(KEY_PROCESS_STEP));
            String nameProdStep = prodName + "_" + stepName;
            
            if (prodVarMap.containsKey(nameProdStep)
                    && procVarMap.containsKey((String)map.get(getAttrNames().get(KEY_PROCESSOR)))) {
                String nameUnitCost = getName(map);
                costMap.put(nameUnitCost, (Integer) map.get(getAttrNames().get(KEY_PROCESS_COST)));
                unitValueMap.put(nameUnitCost, (Integer) map.get(getAttrNames().get(KEY_UNIT_VALUE)));
                                
                // Create map of all Solution IntegerVariables
                IntegerVariable intvar = Choco.makeIntVar(nameUnitCost, 
                        lowValue, highValue, Options.V_BOUND);
                intVarMap.put(nameUnitCost, intvar);
                
                // Add constraints for fixed values
                //==============================================================
                if(((Integer)map.get(getAttrNames().get(KEY_FIXED))) > 0){
                    model.addConstraint(Choco.eq(intvar, 
                            (Integer)map.get(getAttrNames().get(KEY_PROD_VALUE))));
                }

                prodVarMap.get(nameProdStep).add(nameUnitCost);
                procVarMap.get((String) map.get(getAttrNames().get(KEY_PROCESSOR)))
                        .add(nameUnitCost);
            }
        }        
        logger.log(Level.INFO, prodVarMap.toString());
        logger.log(Level.INFO, procVarMap.toString());
        
        return intVarMap;
    }

    /**
     * 
     * @param xmlString
     * @return 
     */
    public synchronized List<Map<String, Object>> getXmlStringAsList(String xmlString) 
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);
        
//        logger.log(Level.INFO, xmlString, xmlString);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));
        
        List<Map<String, Object>> mapList = listMapFromXmlObject(xmlDoc);

        return mapList;
    }
    
    public synchronized List<Map<String, Object>> getXmlFileAsList(String xmlFileUrl, String userName, String password) 
            throws ParserConfigurationException, SAXException, IOException {
      
        Sardine sardine = SardineFactory.begin(userName, password);

        InputStream isData = sardine.get(xmlFileUrl);
 
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        
        Document xmlDoc = builder.parse(new File(xmlFileUrl)
//                new InputSource(isData)
                );
        
        List<Map<String, Object>> mapList = listMapFromXmlObject(xmlDoc);

        return mapList;
    } 

    /**
     * 
     * @param xmlString
     * @param keyAttrName
     * @return 
     */
    public synchronized Map<String, Map<String, Object>> getXmlStringAsMap(String xmlString, String keyAttrName) 
            throws ParserConfigurationException, SAXException, IOException {

        String cleanString = clean(xmlString);
        
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmlDoc = builder.parse(new InputSource(new StringReader(cleanString)));
        
        Map<String, Map<String, Object>> mapList = mapMapFromXmlObject(xmlDoc, keyAttrName);

        return mapList;
    }
  
    public synchronized Map<String, Map<String, Object>> getXmlFileAsMap(String xmlFileUrl, 
            String keyAttrName, String userName, String password) 
            throws ParserConfigurationException, SAXException, IOException {
      
        Sardine sardine = SardineFactory.begin(userName, password);

        InputStream isData = sardine.get(xmlFileUrl);
 
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        
        Document xmlDoc = builder.parse(new InputSource(isData));
        
        Map<String, Map<String, Object>> map = mapMapFromXmlObject(xmlDoc, keyAttrName);

        return map;
    } 
    
    private synchronized void processNode(NodeList nodList, String keyAttrName, Map<String, Map<String, Object>> mapList) 
            throws DOMException, NumberFormatException {
        Node body = nodList.item(0);
        NodeList bodyList = body.getChildNodes();
        int bodyListLenght = bodyList.getLength();
        if (bodyListLenght > 0) {
            for (int i = 0; i < bodyListLenght; i++) {
                Node item = bodyList.item(i);
                NamedNodeMap itemMap = item.getAttributes();
                if (itemMap == null) {
                    System.out.println(item.getNodeName() + " Map Node[" + i + "] item is null!!!");
                    continue;
                }
                Map<String, Object> dataItem = getGridItemMap(itemMap);
                mapList.put(itemMap.getNamedItem(keyAttrName).getTextContent(), dataItem);
            }
        }
    }

    public String formatOutput(List<Map<String, Object>> dataXmlList) {
            StringBuilder stringBuilder = new StringBuilder();
            
            stringBuilder.append("<?xml version=\"1.0\"?>")
                    .append("<Grid>")
                    .append("<Body>")
                    .append("<B>");
            
            for(Map<String, Object> map : dataXmlList){
                stringBuilder.append("<I");
                for (Entry<String, Object> entry : map.entrySet()) {
                    stringBuilder
                            .append(" ")
                            .append(entry.getKey())
                            .append("=\"")
                            .append(entry.getValue())
                            .append("\"")                            
                            ;
                }
                stringBuilder.append("/>");                
            }
            stringBuilder.append("</B>")
                    .append("</Body>")
                    .append("</Grid>");
            
            return stringBuilder.toString();
        }

    public synchronized void saveStringAsFile(String outputString, String fileName) throws IOException {
            
            try {
                Sardine sardine = SardineFactory.begin();
                if (sardine.exists(fileName)) {
                    sardine.delete(fileName);
                }
                sardine.put(fileName, outputString.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                Exceptions.printStackTrace(ex);
            } catch (SardineException ex) {
                Exceptions.printStackTrace(ex);
            }
        }
    
    /**
     * @return the attrNames
     */
    public Map<String, String> getAttrNames() {
        return this.attrNames;
    }

    /**
     * @param attrNames the attrNames to set
     */
    public void setAttrNames(Map<String, String> attrNames) {
        this.attrNames = attrNames;
    }

    private List<Map<String, Object>> listMapFromXmlObject(Document xmlDoc) throws DOMException, NumberFormatException {
        //            Document xmlDoc = XmlTreeGridUtils.parseXML(xmlString.trim());
        Element rootElement = xmlDoc.getDocumentElement();
        List<Map<String, Object>> mapList = Lists.newArrayList();
        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                Node body = gridList.item(0);
                NodeList bodyList = body.getChildNodes();
                int bodyListLenght = bodyList.getLength();
                if (bodyListLenght > 0) {
                    for (int i = 0; i < bodyListLenght; i++) {
                        Node item = bodyList.item(i);
                        NamedNodeMap itemMap = item.getAttributes();

                        if (itemMap == null) {
                            System.out.println("List Node[" + i + "] item is null!!!");
                            continue;
                        }
                        Map<String, Object> dataItem = getGridItemMap(itemMap);
                        mapList.add(dataItem);
                    }
                }
            }
        }
        return mapList;
    }

    private Map<String, Map<String, Object>> mapMapFromXmlObject(Document xmlDoc, String keyAttrName) 
            throws DOMException, NumberFormatException {
        Element rootElement = xmlDoc.getDocumentElement();
        Map<String, Map<String, Object>> mapList = Maps.newHashMap();
        NodeList itemList = rootElement.getChildNodes();
        for (int j = 0; j < itemList.getLength(); j++) {
            Node grid = itemList.item(j);
            NodeList gridList = grid.getChildNodes();
            if (gridList.getLength() > 0) {
                processNode(gridList, keyAttrName, mapList);
            }
        }
        return mapList;
    }

    private int getUpperBound(Map<String, Map<String, Object>> planDataMap) {
        int upperBound = 0;
        
        for(Entry entry : planDataMap.entrySet()){
            int plan = (Integer)((Map)entry.getValue()).get(getAttrNames().get(KEY_PLAN_VALUE));
            if(plan > upperBound){
                upperBound = plan;
            }
        }
        
        return upperBound;
    }

    /**
     * @return the optiMode
     */
    public String getOptiMode() {
        return optiMode;
    }

    /**
     * @param optiMode the optiMode to set
     */
    public void setOptiMode(String optiMode) {
        this.optiMode = optiMode;
    }

    private int convert2Int(Object object) {
        if(object instanceof Integer){
            return (Integer) object;
        } else {
            return Integer.parseInt((object + "").replace(",", "").split("\\.")[0]);
        }
    }

}
