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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
//import org.openl.ie.constrainer.Constrainer;
//import org.openl.ie.constrainer.IntExpArray;
//import org.openl.ie.constrainer.IntVar;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class ConstraintUtils {
    
    public static final String PROD         = "prod";
    public static final String STEP         = "step";
    public static final String MACHINE      = "machine";
    public static final String PROFIT       = "profit";
    public static final String UNIT_VALUE   = "unitvalue";
    private static final String VALUE       = "value";
    public static final String COST         = "cost";
    
    public static final Integer LOW         = 0;
    public static final Integer HIGH        = 200;
    public static final Integer HIGH_COST   = 100000;    
    
    public static void main(String[] args){
        
        List<Map<String, Object>> data          = getTestData();
        
        Map<String, List<String>> prodVarMap    = createProdMap();
        Map<String, List<String>> machineVarMap = createMachineMap();
        
        // Make a map to collect profit and unit values
        Map<String, Integer> profitMap      = Maps.newHashMap();
        Map<String, Integer> unitValueMap   = Maps.newHashMap();
//        Map<String, IntVar> intVarMap       = Maps.newHashMap();
        
        // Create constrainer
//        Constrainer model = new Constrainer("SIMPP");
        
        // Collect variables for products and machines
//        for(Map<String, Object> map : data){
//            // Use only those data that presented in Product and Machine Lists
//            if (prodVarMap.containsKey((String)map.get(PROD))
//                    && machineVarMap.containsKey((String)map.get(MACHINE))) {
//                String name = getName(map);
//                profitMap.put(name, (Integer) map.get(PROFIT));
//                unitValueMap.put(name, (Integer) map.get(UNIT_VALUE));
//                
//                // Create map of all Solution IntegerVariables
//                IntVar intvar = (IntVar) model.addIntVar(LOW, HIGH, name);
//                intVarMap.put(name, intvar);
//
//                prodVarMap.get((String) map.get(PROD)).add(name);
//                machineVarMap.get((String) map.get(MACHINE)).add(name);
//            }
//        }
        
        // Converting collections to arrays and IntegerVariables
        //======================================================================
        
        // Creating Cost criteria variable
//        IntVar cost = (IntVar) model.addIntVar(1, HIGH_COST, COST);
        
//        // Machines
//        Map<String, Pair<int[], IntVar[]>> machineVarMapArray = 
//                createVarMapArray(machineVarMap, unitValueMap, intVarMap);
//        
//        // Products
//        Map<String, Pair<int[], IntVar[]>> prodVarMapArray = 
//                createVarMapArray(prodVarMap, unitValueMap, intVarMap);
//        
//        // create profits and unitValues  arrays
//        IntExpArray varMapArray =
//                mergeVarMapArray(profitMap, intVarMap);
        
        // Now we are ready to make a model
        //======================================================================
               
        // Add products constraints to the model
//        for(Entry entry : prodVarMapArray.entrySet()){
//            String name     = (String) entry.getKey();
//            int[] values    = (int[]) ((Pair)entry.getValue()).getFirst();
//            IntVar[] intVars = (IntVar[]) ((Pair)entry.getValue()).getSecond();
//            IntExpArray intexps = new IntExpArray(model, intVars);
//            int lowB = (int) ((int)getProdMap().get(name) * .8);
//            int uppB = (int) ((int)getProdMap().get(name) * 1.2);
//            IntVar intVar = (IntVar) model.addIntVar(lowB, uppB, name);
////            model.addConstraint(Choco.eq(Choco.scalar(values, intVars), intVar));
////            int upRange = (int)getProdMap().get(name);
////            model.postConstraint(intVar.eq(intVars., values));
//        }
//        
//        // Add machines constraints to the model
//        for(Entry entry : machineVarMapArray.entrySet()){
//            String name     = (String) entry.getKey();
//            int[] values    = (int[]) ((Pair)entry.getValue()).getFirst();
//            IntVar[] intVars = (IntVar[]) ((Pair)entry.getValue()).getSecond();
//            model.addConstraint(Choco.leq(Choco.scalar(values, intVars), (int)getMachineMap().get(name)));
//        }
//        
        // Add fixed values constraints ti the model
        // To Be Implemented (TBI)
        
        // Add cost criteria constrains
        // Plan should maximize profit
//        int[] values =  varMapArray.getFirst();
//        IntVar[] intVars = varMapArray.getSecond();
//        model.addConstraint(Choco.geq(Choco.scalar(values, intVars), cost));
//         
//        Solver solver = new CPSolver();
//        solver.read(model);
////        solver.setValIntIterator(new DecreasingDomain());
//        solver.maximize(solver.getVar(cost), false);

    }

    private static List<Map<String, Object>> getTestData(){
        List<Map<String, Object>> listByProd = Lists.newArrayList();
        
        Map<String, Object> planVars1_1_2 = Maps.newHashMap();        
        planVars1_1_2.put("prod", "prod1");
        planVars1_1_2.put("step", 1);
        planVars1_1_2.put("machine", "machine2");
        planVars1_1_2.put("value", 0);
        planVars1_1_2.put("profit", 10);
        planVars1_1_2.put("unitvalue", 20);
        listByProd.add(planVars1_1_2);
        
        Map<String, Object> planVars1_2_1 = Maps.newHashMap();   
        planVars1_2_1.put("prod", "prod1");
        planVars1_2_1.put("step", 2);
        planVars1_2_1.put("machine", "machine1");
        planVars1_2_1.put("value", 0);
        planVars1_2_1.put("profit", 11);
        planVars1_2_1.put("unitvalue", 21);
        listByProd.add(planVars1_2_1);
        
        Map<String, Object> planVars1_2_3 = Maps.newHashMap();
        planVars1_2_3.put("prod", "prod1");
        planVars1_2_3.put("step", 2);
        planVars1_2_3.put("machine", "machine3");
        planVars1_2_3.put("value", 0);
        planVars1_2_3.put("profit", 12);
        planVars1_2_3.put("unitvalue", 22);
        listByProd.add(planVars1_2_3);
        
        Map<String, Object> planVars1_3_2 = Maps.newHashMap();   
        planVars1_3_2.put("prod", "prod1");
        planVars1_3_2.put("step", 3);
        planVars1_3_2.put("machine", "machine2");
        planVars1_3_2.put("value", 0);
        planVars1_3_2.put("profit", 9);
        planVars1_3_2.put("unitvalue", 19);
        listByProd.add(planVars1_3_2);
        
        Map<String, Object> planVars1_3_3 = Maps.newHashMap(); 
        planVars1_3_3.put("prod", "prod1");
        planVars1_3_3.put("step", 3);
        planVars1_3_3.put("machine", "machine3");
        planVars1_3_3.put("value", 0);
        planVars1_3_3.put("profit", 12);
        planVars1_3_3.put("unitvalue", 22);
        listByProd.add(planVars1_3_3);
        
        Map<String, Object> planVars2_1_2 = Maps.newHashMap();   
        planVars2_1_2.put("prod", "prod2");
        planVars2_1_2.put("step", 1);
        planVars2_1_2.put("machine", "machine2");
        planVars2_1_2.put("value", 0);
        planVars2_1_2.put("profit", 10);
        planVars2_1_2.put("unitvalue", 20);
        listByProd.add(planVars2_1_2);
        
        Map<String, Object> planVars2_1_3 = Maps.newHashMap();
        planVars2_1_3.put("prod", "prod2");
        planVars2_1_3.put("step", 1);
        planVars2_1_3.put("machine", "machine3");
        planVars2_1_3.put("value", 0);
        planVars2_1_3.put("profit", 12);
        planVars2_1_3.put("unitvalue", 22);
        listByProd.add(planVars2_1_3);
        
        Map<String, Object> planVars2_2_1 = Maps.newHashMap();   
        planVars2_2_1.put("prod", "prod2");
        planVars2_2_1.put("step", 2);
        planVars2_2_1.put("machine", "machine1");
        planVars2_2_1.put("value", 0);
        planVars2_2_1.put("profit", 12);
        planVars2_2_1.put("unitvalue", 22);
        listByProd.add(planVars2_2_1);
        
        Map<String, Object> planVars2_2_3 = Maps.newHashMap();
        planVars2_2_3.put("prod", "prod2");   
        planVars2_2_3.put("step", 2);
        planVars2_2_3.put("machine", "machine3");
        planVars2_2_3.put("value", 0);
        planVars2_2_3.put("profit", 11);
        planVars2_2_3.put("unitvalue", 21);
        listByProd.add(planVars2_2_3);
        
        Map<String, Object> planVars3_1_2 = Maps.newHashMap();   
        planVars3_1_2.put("prod", "prod3");
        planVars3_1_2.put("step", 1);
        planVars3_1_2.put("machine", "machine2");
        planVars3_1_2.put("value", 0);
        planVars3_1_2.put("profit", 11);
        planVars3_1_2.put("unitvalue", 21);
        listByProd.add(planVars3_1_2);
        
        Map<String, Object> planVars3_1_3 = Maps.newHashMap(); 
        planVars3_1_3.put("prod", "prod3");
        planVars3_1_3.put("step", 1);
        planVars3_1_3.put("machine", "machine3");
        planVars3_1_3.put("value", 0);
        planVars3_1_3.put("profit", 10);
        planVars3_1_3.put("unitvalue", 20);
        listByProd.add(planVars3_1_3);
        
        Map<String, Object> planVars3_2_1 = Maps.newHashMap();   
        planVars3_2_1.put("prod", "prod3");
        planVars3_2_1.put("step", 2);
        planVars3_2_1.put("machine", "machine1");
        planVars3_2_1.put("value", 0);
        planVars3_2_1.put("profit", 9);
        planVars3_2_1.put("unitvalue", 19);
        listByProd.add(planVars3_2_1);
        
        Map<String, Object> planVars3_2_2 = Maps.newHashMap(); 
        planVars3_2_2.put("prod", "prod3");
        planVars3_2_2.put("step", 2);
        planVars3_2_2.put("machine", "machine2");
        planVars3_2_2.put("value", 0);
        planVars3_2_2.put("profit", 12);
        planVars3_2_2.put("unitvalue", 22);
        listByProd.add(planVars3_2_2);
        
        return listByProd;
    }
    
    private static Map<String, Integer> getPlanMap(){
        Map<String, Integer> planMap = Maps.newHashMap();
        planMap.put("prod1", 154);
        planMap.put("prod2", 184);
        planMap.put("prod3", 128);
        
        return planMap;
    }
    
    private static Map<String, Integer> getProdMap(){
        Map<String, Integer> planMap = getPlanMap();
        
        Map<String, Integer> prodMap = Maps.newHashMap();
        
        return prodMap;
    }
    
    private static Map<String, Integer> getMachineMap(){
        Map<String, Integer> machineMap = Maps.newHashMap();
        machineMap.put("machine1", 212);
        machineMap.put("machine2", 256);
        machineMap.put("machine3", 272);
        
        return machineMap;
    }

    private static Map<String, List<String>> createProdMap() {
        // Map to collect variables for prducts
        Map<String, List<String>> prodVarMap = Maps.newHashMap();
        for(Entry<String, Integer> item : getProdMap().entrySet()){
            List<String> varList = Lists.newArrayList();
            prodVarMap.put(item.getKey(), varList);
        }
        return prodVarMap;
    }

    private static Map<String, List<String>> createMachineMap() {
        // Map to collect Machine variables
        Map<String, List<String>> machineVarMap = Maps.newHashMap();
        for(Entry<String, Integer> item : getMachineMap().entrySet()){
            List<String> varList = Lists.newArrayList();
            machineVarMap.put(item.getKey(), varList);
        }
        return machineVarMap;
    }

//    private static Map<String, IntExpArray> createVarMapArray(
//            Map<String, List<String>> varMap, 
//            Map<String, Integer> unitValueMap,
//            Map<String, IntVar> intVarMap,
//            Constrainer constr) {
//        
//        Map<String, IntExpArray> varMapArray = Maps.newHashMap();
//        
//        for (Entry entry : varMap.entrySet()) {
//            String name     = (String) entry.getKey();
//            List varList    = (List)varMap.get(name);
//            int uvaSize     = (int)varList.size();
//            
//            IntExpArray expArray = new IntExpArray(constr, uvaSize);
//            for(int i = 0; i < uvaSize; i++){
//                String varName = (String) varList.get(i);
//                int value   = unitValueMap.get(varName);
//                IntVar var  = intVarMap.get(varName);
//                
//                expArray.set(var.mul(value), i);
//            } 
//            
//            varMapArray.put(name, expArray);
//        }
//        
//        return varMapArray;
//    }

//    private static IntExpArray mergeVarMapArray(
//            Map<String, Integer> profitMap, Map<String, IntVar> unitValueMap,
//            Constrainer constr) {
//       
//        int arraySize           = profitMap.size();
//        IntExpArray expArray    = new IntExpArray(constr, arraySize);
//        
//        int[] profits       = new int[arraySize];
//        IntVar[] intVars    = new IntVar[arraySize];
//        
//        int i = 0;
//        for (Entry entry : profitMap.entrySet()) {
//            String name = (String) entry.getKey();
//            int value   = (Integer)entry.getValue();
//            IntVar var  = unitValueMap.get(name);
//            expArray.set(var.mul(value), i);
//            i++;
//        }
//        
//        return expArray;
//    }

//    private static List<Map<String, Object>> updateData(
//            List<Map<String, Object>> data, 
//            Map<String, IntVar> intVarMap,
////            Solver solver) {
//        
////        for(Map<String, Object> item : data){
////            String name = getName(item);
////            IntVar intVar = intVarMap.get(name); 
////            item.put(VALUE, solver.getVar(intVar).getVal());
////        }
////        
////        return data;
//    }
//
//    private static String getName(Map<String, Object> map) {
//        String name = (String) map.get(PROD) + "_"
//                + (Integer) map.get(STEP) + "_"
//                + (String) map.get(MACHINE);
//        return name;
//    }

}
