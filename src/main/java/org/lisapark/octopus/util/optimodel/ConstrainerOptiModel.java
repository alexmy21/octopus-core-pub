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
package org.lisapark.octopus.util.optimodel;

import choco.Choco;
import choco.Options;
import choco.kernel.model.Model;
import choco.kernel.model.variables.integer.IntegerExpressionVariable;
import choco.kernel.model.variables.integer.IntegerVariable;
import choco.kernel.solver.Solver;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ConstrainerOptiModel {
    
    public static final int PLAN_TYPE_AGGRESSIVE        = 1;
    public static final int PLAN_TYPE_BALANCED          = 2;
    public static final int PLAN_TYPE_CAUTIOUS          = 3;
    
//    public static final int MEASURE_COEFFICIENT_DEFAULT = 1;
    
    private List<OptiBean>          dataOptiBeanList;
    private Map<String,OptiBean>    dataOptiBeanMap;
    private Map<String, OptiBean>   planOptiBeanMap;
    private Map<String, OptiBean>   resourcesOptiBenMap;
    
    private List<Constant>          planConstantList;
    private List<Constant>          resourceConstantList;
    
    private Map<String, Constant>   planConstantMap;
    private Map<String, Constant>   resourceConstantMap;
    
    private static final Integer    UPPER_COEFFICIENT   = 1;
    private static final Double     LOW_COEFFICIENT     = .01;
    private static final Integer    ONE_CONST           = 1;
    private static final Integer    PROD_ABS_LOW        = 10;
    
    // Singleton IntegerVariable arrays   
    private IntegerVariable[] intVarPlanArray;
    private IntegerVariable[] intVarResourceArray;
    private IntegerVariable[] intVarDataAbsArray;
    private IntegerVariable[] intVarDataUnitArray;
    
    private int planType;
    private int prodAbsLow;
//    private int measureCoefficient; 
    
    
    
    /**
     * 
     * @param data
     * @param plan
     * @param resources 
     */
    public ConstrainerOptiModel(
            List<OptiBean>          dataOptiBeanList, 
            Map<String, OptiBean>   planOptiBeanMap, 
            Map<String, OptiBean>   resourcesOptiBeanMap
            ){
        
        this.dataOptiBeanList       = dataOptiBeanList;
        this.dataOptiBeanMap        = createdataOptiBeanMap(dataOptiBeanList);
        
        this.planOptiBeanMap        = planOptiBeanMap;
        this.resourcesOptiBenMap    = resourcesOptiBeanMap;
        
        this.planConstantList       = assignPlanConstantList(planOptiBeanMap);
        this.resourceConstantList   = assignResourceConstantList(resourcesOptiBeanMap);
        
        this.planConstantMap        = assignPlanConstantMap(planOptiBeanMap);
        this.resourceConstantMap    = assignResourceConstantMap(resourcesOptiBeanMap);
       
    }

    /**
     * 
     * @param dataOptiBenList
     * @return 
     */
    private Map<String, OptiBean> createdataOptiBeanMap(List<OptiBean> dataOptiBenList) {
        
        Map<String, OptiBean> map = Maps.newHashMap();
        for(OptiBean bean : dataOptiBenList){
            map.put(bean.getFullName(), bean);
        }
        
        return map;
    }

    
    // Constraint builders
    //==========================================================================
    /**
     * 
     * @return 
     */
    public IntegerExpressionVariable    createDataTotalCostIntExpVariable(){
        
        IntegerVariable[] intVar = getIntVarDataUnitArray();
        List<OptiBean> data = this.getDataOptiBeanList();
        int[] costs = new int[intVar.length];
        
        for(int i = 0; i < costs.length; i++){
            costs[i] = data.get(i).getProcessCost() * data.get(i).getUnitValue();
        }
        
        return Choco.scalar(intVar, costs);
    }
    
    /**
     * 
     * @return 
     */
    public IntegerExpressionVariable    createResourceTotalIntExtVariable(){
       
        List<Constant> resources = this.getResourceConstantList();         
        IntegerExpressionVariable[] expVar = new IntegerExpressionVariable[resources.size()];
        
        int i = 0;
        for(Constant item : resources){
            IntegerExpressionVariable resource = createResourceIntExpVariable(item.getName());
            expVar[i] = resource;
            i++;
        }
        return Choco.sum(expVar);
    }
    
    /**
     * 
     * @param plan
     * @return 
     */
    private IntegerExpressionVariable[] createPlanIntExpVariable(String plan){
        
        IntegerVariable[] intVar        = getIntVarDataUnitArray();
        
        Map<String, List<IntegerExpressionVariable>> listMap =
                Maps.newHashMap();
        
        for(int i = 0; i < intVar.length; i++){
            OptiBean optiBean = getDataOptiBeanList().get(i);
            String name = optiBean.getProd();
            String technologyStepName = optiBean.getProdStepName();
            
            // Find list in listMap that collects vers for the
            // current prodStep intVarExp
            //==================================================================
            if (name != null && name.equalsIgnoreCase(plan)) {
                if (!listMap.containsKey(technologyStepName)) {
                    List<IntegerExpressionVariable> planVars = Lists.newArrayList();
                    listMap.put(technologyStepName, planVars);
                }
                int unitValue = optiBean.getUnitValue();
                // Calculate abs value for product plan
                listMap.get(technologyStepName).add(Choco.mult(intVar[i], unitValue));
            }
        }        
        
        // Go over the map of Integer Variables that referenced fron the same tech step
        // for specific product
        IntegerExpressionVariable[] intExpVars = new IntegerExpressionVariable[listMap.size()];
        int i = 0;
        for (Entry<String, List<IntegerExpressionVariable>> entry : listMap.entrySet()) {
            List<IntegerExpressionVariable> list = entry.getValue();
            IntegerExpressionVariable[] technologySteps = new IntegerExpressionVariable[list.size()];
            technologySteps = list.toArray(technologySteps);
            intExpVars[i] = (IntegerExpressionVariable) Choco.sum(technologySteps);
            i++;
        }
        
        return intExpVars;
    }
     
    /**
     * 
     * @param resource
     * @return 
     */
    private IntegerExpressionVariable   createResourceIntExpVariable(String resource){
       
        IntegerVariable[] intVar = getIntVarDataUnitArray();
        List<IntegerVariable> resVars = Lists.newArrayList();
        
        for(int i = 0; i < intVar.length; i++){
            String name = getDataOptiBeanList().get(i).getResource();
            if (name != null && name.equalsIgnoreCase(resource)) {
                resVars.add(intVar[i]);
            }
        }
        
        IntegerVariable[] resources = new IntegerVariable[resVars.size()];
        resources = resVars.toArray(resources);
        
        return Choco.sum(resources);
    }
    
    // Model Properties
    //==========================================================================
    /**
     * @return the productConstantList
     */
    public List<Constant>           getPlanConstantList() {
        return planConstantList;
    }

    /**
     * @return the resourceConstantList
     */
    public List<Constant>           getResourceConstantList() {
        return resourceConstantList;
    }

    /**
     * @return the productConstantMap
     */
    public Map<String, Constant>    getPlanConstantMap() {
        return planConstantMap;
    }

    /**
     * @return the resourceConstantMap
     */
    public Map<String, Constant>    getResourceConstantMap() {
        return resourceConstantMap;
    } 
   
    /**
     * @return the dataOptiBenMap
     */
    public List<OptiBean>           getDataOptiBeanList() {
        return dataOptiBeanList;
    }

    /**
     * @return the planOptiBenMap
     */
    public Map<String, OptiBean>    getPlanOptiBeanMap() {
        return planOptiBeanMap;
    }

    /**
     * @return the resourcesOptiBenMap
     */
    public Map<String, OptiBean>    getResourcesOptiBeanMap() {
        return resourcesOptiBenMap;
    }
 
    // IntegerVariable Arrays
    //==========================================================================
 
    /**
     * @return the intVarPlanArray
     */
    public IntegerVariable[] getIntVarPlanArray() {
        if(intVarPlanArray == null){
            intVarPlanArray = this.createIntVarPlanArray();
        }
        return intVarPlanArray;
    }

    /**
     * @return the intVarResourceArray
     */
    public IntegerVariable[] getIntVarResourceArray() {
        if(intVarResourceArray == null){
            intVarResourceArray = this.createIntVarResourceArray();
        }
        return intVarResourceArray;
    }

    /**
     * @return the intVarDataAbsArray
     */
    public IntegerVariable[] getIntVarDataAbsArray() {
        if(intVarDataAbsArray == null){
            intVarDataAbsArray = this.createIntVarDataAbsArray();
        }
        return intVarDataAbsArray;
    }

    /**
     * @return the intVarDataUnitArray
     */
    public IntegerVariable[] getIntVarDataUnitArray() {
        if(intVarDataUnitArray == null){
            intVarDataUnitArray = this.createIntVarDataUnitArray();
        }
        return intVarDataUnitArray;
    }
    
    // Creating IntegerVariables Arrays
    //==========================================================================
    /**
     * 
     * @param optiModel
     * @param plan
     * @return 
     */
    private IntegerVariable[] createIntVarPlanArray() {
        
        List<Constant> dataList = getPlanConstantList();
        
        IntegerVariable[] intVars = new IntegerVariable[dataList.size()];
        
        int i = 0;
        for (Iterator<Constant> it = dataList.iterator(); it.hasNext();) {
            Constant item = it.next();
            String name = item.getName();
            Integer lowbound;
            if(item.getLowBound() != null || item.getLowBound() > 0){
                lowbound = item.getLowBound();
            } else {
                lowbound = (int)(item.getValue() * LOW_COEFFICIENT) ;
            }
            Integer upperbound;
            if(item.getUpperBound() != null && item.getUpperBound() > 0){
                upperbound = item.getUpperBound();
            } else {
                upperbound = item.getValue() * UPPER_COEFFICIENT;
            }
            intVars[i] = Choco.makeIntVar(name, (int)lowbound, (int)upperbound, Options.V_BOUND);
            i++;
        }
        return intVars;
    }

    /**
     * 
     * @param optiModel
     * @param machine
     * @return 
     */
    private IntegerVariable[] createIntVarResourceArray() {
         
        List<Constant> dataList = getResourceConstantList();
        
        IntegerVariable[] intVars = new IntegerVariable[dataList.size()];
        
        int i = 0;
        for (Iterator<Constant> it = dataList.iterator(); it.hasNext();){
            Constant item = it.next();
            String name = item.getName();
            Integer lowbound;
            if(item.getLowBound() != null || item.getLowBound() > 0){
                lowbound = item.getLowBound();
            } else {
                lowbound = (int)(item.getValue() * LOW_COEFFICIENT);
            }
            
            Integer upperbound;
            if(item.getUpperBound() != null && item.getUpperBound() > 0){
                upperbound = item.getUpperBound();
            } else {
                upperbound = item.getValue() * UPPER_COEFFICIENT;
            }
                        
            intVars[i] = Choco.makeIntVar(name, (int)lowbound, (int)upperbound, Options.V_BOUND);
            
            i++;
        }
        return intVars;
    }

    /**
     * 
     * @param plan
     * @return 
     */
    private IntegerVariable[] createIntVarDataAbsArray() {
        
        List<OptiBean> dataList = this.getDataOptiBeanList();
        Map<String, Constant> plan = getPlanConstantMap();
        
        IntegerVariable[] intVars = new IntegerVariable[dataList.size()];
        
        int i = 0;
        for(OptiBean item : dataList){
            String name = item.getFullName();
            String planName = item.getProd();
            
            Integer lowbound = (item.getLowBound() != null && item.getLowBound() > 0)
                    ? item.getLowBound() : (int)(plan.get(planName).getValue() * LOW_COEFFICIENT);
            
            Integer upperbound = (item.getUpperBound() != null && item.getUpperBound() > 0)
                    ? item.getUpperBound() : plan.get(planName).getValue() * UPPER_COEFFICIENT;
            
            intVars[i] = Choco.makeIntVar(name, (int)lowbound, 
                    (int)upperbound, Options.V_BOUND);
            
            i++;
        }
        
        return intVars;
    }  
    
    /**
     * 
     * @param plan
     * @return 
     */
    private IntegerVariable[] createIntVarDataUnitArray() {
        
        List<OptiBean> dataList = this.getDataOptiBeanList();
        Map<String, Constant> plan = getPlanConstantMap();
        
        IntegerVariable[] intVars = new IntegerVariable[dataList.size()];
        
        int i = 0;
        for(OptiBean item : dataList){
            String name = item.getFullName();
            String planName = item.getProd();
            
            Integer unitvalue = item.getUnitValue() == null ? 1 
                    : (item.getUnitValue() == 0 ? ONE_CONST : item.getUnitValue());
            
            Integer lowbound = (item.getLowBound() != null && item.getLowBound() > 0)
                    ? item.getLowBound() : (int)(plan.get(planName).getValue() * LOW_COEFFICIENT)/unitvalue;
//            if(lowbound < getProdAbsLow()){
//                lowbound = getProdAbsLow();
//            }
            Integer upperbound = (item.getUpperBound() != null && item.getUpperBound() > 0)
                    ? item.getUpperBound() : (plan.get(planName).getValue() * UPPER_COEFFICIENT) /unitvalue;
            
//            if (upperbound < lowbound){
//                intVars[i] = Choco.ZERO;
//            } else {
                intVars[i] = Choco.makeIntVar(name, (int)lowbound, (int)upperbound, Options.V_BOUND);
//            }
            
            i++;
        }
        
        return intVars;
    }
    
    // Initial assignmeny
    //==========================================================================
    /**
     * 
     * @param data
     * @return 
     */
    private List<Constant>          assignPlanConstantList(Map<String, OptiBean> planData) {
        
        List<Constant> constantList = Lists.newArrayList();
        
        for(Entry<String, OptiBean> entry : planData.entrySet()){
            String name = entry.getKey();
            OptiBean optiBean = entry.getValue();
            Constant constant = new Constant();
            
            if(optiBean.getProd() != null && !optiBean.getProd().isEmpty()) { 
                constant.setName(optiBean.getProd());            
                constant.setValue(optiBean.getPlanValue());             
            } else {
                continue;
            }
            constant.setLowBound(optiBean.getLowBound());
            constant.setUpperBound(optiBean.getUpperBound());
            
            constantList.add(constant);
        }
        
        return constantList;
    }
    
    /**
     * 
     * @param data
     * @return 
     */
    private List<Constant>          assignResourceConstantList(Map<String, OptiBean> resourceData) {
        
        List<Constant> constantList = Lists.newArrayList();
        
        for(Entry<String, OptiBean> entry : resourceData.entrySet()){
//            String name = entry.getKey();
            OptiBean optiBean = entry.getValue();
            Constant constant = new Constant();
            
            if(optiBean.getResource() != null && !optiBean.getResource().isEmpty()) { 
                constant.setName(optiBean.getResource());            
                constant.setValue(optiBean.getResourceValue());                
            } else {
                continue;
            }
            constant.setLowBound(optiBean.getLowBound());
            constant.setUpperBound(optiBean.getUpperBound());
            
            constantList.add(constant);
        }
        
        return constantList;
    }
   
    /**
     * 
     * @param data
     * @return 
     */
    private Map<String, Constant>   assignPlanConstantMap(Map<String, OptiBean> planData) {
        Map<String, Constant> constantMap = Maps.newHashMap();
        
        for(Entry<String, OptiBean> entry : planData.entrySet()){
            String name = entry.getKey();
            OptiBean optiBean = entry.getValue();
            Constant constant = new Constant();

            constant.setName(optiBean.getProd());
            constant.setValue(optiBean.getPlanValue());

            constant.setLowBound(optiBean.getLowBound());
            constant.setUpperBound(optiBean.getUpperBound());
            
            constantMap.put(name, constant);
        }
        
        return constantMap;
    }
    
    /**
     * 
     * @param data
     * @return 
     */
    private Map<String, Constant>   assignResourceConstantMap(Map<String, OptiBean> resourceData) {
        Map<String, Constant> constantMap = Maps.newHashMap();
        
        for(Entry<String, OptiBean> entry : resourceData.entrySet()){
            String name = entry.getKey();
            OptiBean optiBean = entry.getValue();
            Constant constant = new Constant();

            constant.setName(optiBean.getResource());
            constant.setValue(optiBean.getResourceValue());
           
            constant.setLowBound(optiBean.getLowBound());
            constant.setUpperBound(optiBean.getUpperBound());
            
            constantMap.put(name, constant);
        }
        
        return constantMap;
    }
    
    // Adding variables to Choco model
    //==========================================================================
    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addDataAbsVariables(Model chocoModel) {
        
        IntegerVariable[] intVars = this.getIntVarDataAbsArray();
        
        for(int i = 0; i < intVars.length; i++){
            chocoModel.addVariable(intVars[i]);
        }
        
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addDataUnitVariables(Model chocoModel) {
        
        IntegerVariable[] intVars = this.getIntVarDataUnitArray();
        
        for(int i = 0; i < intVars.length; i++){
            chocoModel.addVariable(intVars[i]);
        }
        
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addPlanVariables(Model chocoModel) {        
        
        IntegerVariable[] intVars = this.getIntVarPlanArray();
                
        for(int i = 0; i < intVars.length; i++){
            chocoModel.addVariable(intVars[i]);
        }
        
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addResourceVariables(Model chocoModel) {
        
        IntegerVariable[] intVars = this.getIntVarResourceArray();
                
        for(int i = 0; i < intVars.length; i++){
            chocoModel.addVariable(intVars[i]);
        }
        
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addPlanConstraints(Model chocoModel) {
        
        IntegerVariable[] intVar = getIntVarPlanArray();
        
        for (int i = 0; i < intVar.length; i++) {
            String plan = intVar[i].getName();            
            IntegerExpressionVariable[] expVars = this.createPlanIntExpVariable(plan);

            for (int j = 0; j < expVars.length; j++) {
                switch(getPlanType()){
                    case ConstrainerOptiModel.PLAN_TYPE_AGGRESSIVE:
                        chocoModel.addConstraint(Choco.geq(expVars[j], intVar[i]));
                        break;
                    case ConstrainerOptiModel.PLAN_TYPE_BALANCED:
                        chocoModel.addConstraint(Choco.eq(expVars[j], intVar[i]));
                        break;
                    case ConstrainerOptiModel.PLAN_TYPE_CAUTIOUS:
                        chocoModel.addConstraint(Choco.leq(expVars[j], intVar[i]));
                        break;
                    default:
                        chocoModel.addConstraint(Choco.eq(expVars[j], intVar[i]));
                        break;
                } 
                // All tech steps have to process the same number of products
                if(j > 0) {
                    chocoModel.addConstraint(Choco.eq(expVars[j], expVars[j - 1]));
                }
                
            }
        }
                
        return chocoModel;
    }
    
    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addFixedProdConstraints(Model chocoModel){
        
        IntegerVariable[] intVar = getIntVarDataUnitArray();
        
        for (int i = 0; i < intVar.length; i++) {
            Integer fixed       = getDataOptiBeanList().get(i).getFixed();
            Integer prodValue   = getDataOptiBeanList().get(i).getProdValue();
            if (fixed > 0) {
                chocoModel.addConstraint(Choco.eq(intVar[i], prodValue));
            }
        }
                
        return chocoModel;
    }

    /**
     * 
     * @param chocoModel
     * @return 
     */
    public Model addResourceConstraints(Model chocoModel) {
        
        IntegerVariable[] intVar = getIntVarResourceArray();
        
        for (int i = 0; i < intVar.length; i++) {
            String resource = intVar[i].getName();            
            IntegerExpressionVariable expVar = this.createResourceIntExpVariable(resource);
//            chocoModel.addVariable(expVar);
            chocoModel.addConstraint(Choco.leq(expVar, intVar[i]));
        }
                
        return chocoModel;
    }

    // Utils and some other properties
    //==========================================================================
    /**
     * 
     * @param data
     * @param optiBean
     * @param solver
     * @return 
     */
    public List<Map<String, Object>> updateData(Map<String, Map<String, Object>> data, 
            Map<String, IntegerVariable> intVar,
            String prodName,
            Solver solver) {
        
        List<Map<String, Object>> list = Lists.newArrayList();
               
        if (solver.getNbSolutions() > 0) {
            for (Entry<String, Map<String, Object>> item : data.entrySet()) {
                String name = item.getKey();
                IntegerVariable intVarEntry = intVar.get(name);
                
                if (intVarEntry != null && solver.getVar(intVarEntry) != null) {
                    Integer intValue = solver.getVar(intVarEntry).getVal();
                    item.getValue().put(prodName, intValue);
                }
                
                Map<String, Object> map = Maps.newHashMap(item.getValue());
                list.add(map);
            }
        }
        return list;
    }

    /**
     * @return the dataOptiBeanMap
     */
    public Map<String,OptiBean> getDataOptiBeanMap() {
        return dataOptiBeanMap;
    }

    /**
     * 
     * @param intVars
     * @return 
     */
    private Map<String, IntegerVariable> convertArray2Map(IntegerVariable[] intVars) {
        Map<String, IntegerVariable> map = Maps.newHashMap();
        
        for(int i =0; i < intVars.length; i++){
            map.put(intVars[i].getName(), intVars[i]);
        }
        return map;
    }

    /**
     * 
     * @param map
     * @param optiBean
     * @return 
     */
    private String getName(Map<String, Object> map, OptiBean optiBean) {
        String name = (String) map.get(optiBean.PROD()) + "_"
                + (Integer) map.get(optiBean.PROCESS_STEP()) + "_"
                + (String) map.get(optiBean.PROCESSOR());
        return name;
    }

    public Map<String, IntegerVariable> intVarMap(IntegerVariable[] intVar) {
        Map<String, IntegerVariable> map = Maps.newHashMap();
        
        if(intVar != null){
            for(int i = 0; i < intVar.length; i++){
                map.put(intVar[i].getName(), intVar[i]);
            }
            return map;
        } else {
            return null;
        }
    }

    /**
     * @return the planType
     */
    public int getPlanType() {
        return planType == 0 ?  PLAN_TYPE_CAUTIOUS : planType;
    }

    /**
     * @param planType the planType to set
     */
    public void setPlanType(int planType) {
        this.planType = planType;
    }

    /**
     * @return the prodAbsLow
     */
    public int getProdAbsLow() {
        return prodAbsLow == 0 ? PROD_ABS_LOW : prodAbsLow;
    }

    /**
     * @param prodAbsLow the prodAbsLow to set
     */
    public void setProdAbsLow(int prodAbsLow) {
        this.prodAbsLow = prodAbsLow;
    }

}
