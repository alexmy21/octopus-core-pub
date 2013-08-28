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

import com.google.common.collect.Maps;
import java.util.Map;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public final class OptiBean {
    
//    private String prod;
//    private String processStep;
//    private String processor;
//    private String resource;
//    private Integer processCost;
//    private Integer unitValue;
//    private Integer prodValue;
//    private Integer lowBound;
//    private Integer upperBound;
//    private Integer planValue;
//    private Integer resourceValue;
//    private Integer fixed;    
    
    private String PROD;
    private String PROCESS_STEP;
    private String PROCESSOR;
    private String PROCESS_COST;
    private String UNIT_VALUE;
    private String PROD_VALUE;
    private String LOW_BOUND;
    private String UPPER_BOUND;
    private String PLAN_VALUE;
    private String RESOURCE;
    private String RESOURCE_VALUE;
    private String FIXED;
    private String PROD_RESOURCE_LIMIT; // Product Resource use limit
    
    public static final String KEY_PROD                 = "PROD";
    public static final String KEY_PROCESS_STEP         = "PROCESS_STEP";
    public static final String KEY_PROCESSOR            = "PROCESSOR";
    public static final String KEY_PROCESS_COST         = "PROCESS_COST";
    public static final String KEY_UNIT_VALUE           = "UNIT_VALUE";
    public static final String KEY_PROD_VALUE           = "PROD_VALUE";
    public static final String KEY_LOW_BOUND            = "LOW_BOUND";
    public static final String KEY_UPPER_BOUND          = "UPPER_BOUND";
    public static final String KEY_PLAN_VALUE           = "PLAN_VALUE";
    public static final String KEY_RESOURCE             = "RESOURCE";
    public static final String KEY_RESOURCE_VALUE       = "RESOURCE_VALUE";
    public static final String KEY_FIXED                = "FIXED";
    public static final String KEY_PROD_RESOURCE_LIMIT  = "PROD_RESOURCE_LIMIT";
    
    private Map<String, Object> dataMap;
    
    public static final int PRECISION_DEFAULT = 1;
    private int precisionCoeff;
    
    private static String dict;

//    private OptiBean(Map<String, Object> dataMap){
//        this.dataMap = importMap(dataMap);
//        
//    }    
    
    private OptiBean(Map<String, Object> dataMap, int precisionCoeff, String dict){
        
        if(dict == null) {
            return;
        } else {
            OptiBean.dict = dict;
        }
        
        String[] keyValues = dict.split(",");
        for(String keyValue : keyValues){
            String[] pair = keyValue.split("=");
            parseDict(pair);
        } 
        
        this.dataMap = Maps.newHashMap();
        if(precisionCoeff > 0){
            this.precisionCoeff = precisionCoeff;
        } else {
            this.precisionCoeff = PRECISION_DEFAULT;
        }
        
        if(dataMap != null) {
            importMap(dataMap, this.precisionCoeff);
        }
    }

//    public static OptiBean newInstance(){        
//        return new OptiBean(null);
//    }
//    
//    public static OptiBean newInstance(Map<String, Object> dataMap){        
//        return new OptiBean(dataMap);
//    }
    
    public static OptiBean newInstance(Map<String, Object> dataMap, int precisionCoeff, String dict){        
        return new OptiBean(dataMap, precisionCoeff, dict);
    }
    
    /**
     * @return the prod
     */
    public String getProd() {
        return dataMap.get(PROD) + "";
    }

    /**
     * @return the processStep
     */
    public String getProcessStep() {
        return dataMap.get(PROCESS_STEP) + "";
    }

    /**
     * @return the processor
     */
    public String getProcessor() {
        return dataMap.get(PROCESSOR) + "";
    }

    /**
     * @return the resource
     */
    public String getResource() {
        return dataMap.get(RESOURCE) + "";
    }

    /**
     * @return the processCost
     */
    public Integer getProcessCost() {
        return getInteger(dataMap.get(PROCESS_COST));
//        == null ? null : (Integer) dataMap.get(PROCESS_COST);
    }

    /**
     * @return the unitValue
     */
    public Integer getUnitValue() {
        return getInteger(dataMap.get(UNIT_VALUE));
//        == null ? null : (Integer) dataMap.get(UNIT_VALUE);
    }

    /**
     * @return the prodValue
     */
    public Integer getProdValue() {
        return getInteger(dataMap.get(PROD_VALUE)); 
//        == null ? null : (Integer) dataMap.get(PROD_VALUE);
    }

    /**
     * @return the lowBound
     */
    public Integer getLowBound() {
        return getInteger(dataMap.get(LOW_BOUND));
//        == null ? null : (Integer) dataMap.get(LOW_BOUND);
    }

    /**
     * @return the upperBound
     */
    public Integer getUpperBound() {
        return getInteger(dataMap.get(UPPER_BOUND));
//        == null ? null : (Integer) dataMap.get(UPPER_BOUND);
    }

    /**
     * @return the planValue
     */
    public Integer getPlanValue() {
        return getInteger(dataMap.get(PLAN_VALUE));
//        == null ? null : (Integer) dataMap.get(PLAN_VALUE);
    }

    /**
     * @return the resourceValue
     */
    public Integer getResourceValue() {
        return getInteger(dataMap.get(RESOURCE_VALUE)); 
//                == null ? null : (Integer) dataMap.get(RESOURCE_VALUE);
    }

    /**
     * @return the fixed
     */
    public Integer getFixed() {
        return dataMap.get(FIXED) == null ? null : (Integer) dataMap.get(FIXED);
    }   

    public void importMap(Map<String, Object> dataMap, int  precisionCoeff) { 
       
        if(dataMap == null) {
            return;
        }
        
        this.dataMap.put(PROD, dataMap.get(PROD()) + "");
        this.dataMap.put(PROCESS_STEP, dataMap.get(PROCESS_STEP()) + "");
        this.dataMap.put(PROCESSOR, dataMap.get(PROCESSOR()) + "");
        this.dataMap.put(PROCESS_COST, getInteger(dataMap.get(PROCESS_COST())) + "");
        this.dataMap.put(UNIT_VALUE, getInteger(dataMap.get(UNIT_VALUE())) + "");
        this.dataMap.put(PROD_VALUE, getInteger(dataMap.get(PROD_VALUE())) + "");
        
        if (dataMap.get(LOW_BOUND()) != null) {
            int lowBound = getInteger(dataMap.get(LOW_BOUND())) * precisionCoeff;
            this.dataMap.put(LOW_BOUND, Integer.valueOf(lowBound));
        } else {
            this.dataMap.put(LOW_BOUND, null);
        }
        
        if (dataMap.get(UPPER_BOUND()) != null) {
            int upperBound = getInteger(dataMap.get(UPPER_BOUND())) * precisionCoeff;
            this.dataMap.put(UPPER_BOUND, Integer.valueOf(upperBound));
        } else {
            this.dataMap.put(UPPER_BOUND, null);
        }

        if (dataMap.get(PLAN_VALUE()) != null) {
            int planValue = getInteger(dataMap.get(PLAN_VALUE())) * precisionCoeff;
            this.dataMap.put(PLAN_VALUE, Integer.valueOf(planValue));
        } else {
            this.dataMap.put(PLAN_VALUE, null);
        }

        if (dataMap.get(RESOURCE_VALUE()) != null) {
            int resourceValue = getInteger(dataMap.get(RESOURCE_VALUE())) * precisionCoeff;
            this.dataMap.put(RESOURCE_VALUE, Integer.valueOf(resourceValue));
        } else {
            this.dataMap.put(RESOURCE_VALUE, null);
        }
        
        if (dataMap.get(PROD_RESOURCE_LIMIT()) != null) {
            int resourceLimit = getInteger(dataMap.get(PROD_RESOURCE_LIMIT()));
            this.dataMap.put(PROD_RESOURCE_LIMIT, Integer.valueOf(resourceLimit));
        } else {
            this.dataMap.put(PROD_RESOURCE_LIMIT, null);
        }
        
        this.dataMap.put(RESOURCE, dataMap.get(RESOURCE()) + "");
        this.dataMap.put(FIXED, getInteger(dataMap.get(FIXED())));
        
    }

    /**
     * @return the dataMap
     */
    public Map<String, Object> getDataMap() {
        return Maps.newHashMap(dataMap);
    }

    private void parseDict(String[] pair) {
        if(KEY_PROD.equalsIgnoreCase(pair[0])){
            PROD = pair[1];
        } else if(KEY_PROCESS_STEP.equalsIgnoreCase(pair[0])){
            PROCESS_STEP = pair[1];
        } else if(KEY_PROCESSOR.equalsIgnoreCase(pair[0])){
            PROCESSOR = pair[1];
        } else if(KEY_PROCESS_COST.equalsIgnoreCase(pair[0])){
            PROCESS_COST = pair[1];
        } else if(KEY_UNIT_VALUE.equalsIgnoreCase(pair[0])){
            UNIT_VALUE = pair[1];
        } else if(KEY_PROD_VALUE.equalsIgnoreCase(pair[0])){
            PROD_VALUE = pair[1];
        } else if(KEY_LOW_BOUND.equalsIgnoreCase(pair[0])){
            LOW_BOUND = pair[1];
        } else if(KEY_UPPER_BOUND.equalsIgnoreCase(pair[0])){
            UPPER_BOUND = pair[1];
        } else if(KEY_PLAN_VALUE.equalsIgnoreCase(pair[0])){
            PLAN_VALUE = pair[1];
        } else if(KEY_RESOURCE.equalsIgnoreCase(pair[0])){
            RESOURCE = pair[1];
        } else if(KEY_PROD.equalsIgnoreCase(pair[0])){
            PROD = pair[1];
        } else if(KEY_RESOURCE_VALUE.equalsIgnoreCase(pair[0])){
            RESOURCE_VALUE = pair[1];
        } else if(KEY_PROD_RESOURCE_LIMIT.equalsIgnoreCase(pair[0])){
            PROD_RESOURCE_LIMIT = pair[1];
        } else if(KEY_FIXED.equalsIgnoreCase(pair[0])){
            FIXED = pair[1];
        } else {
            
        }
    }

    /**
     * @return the PROD
     */
    public String PROD() {
        return PROD;
    }

    /**
     * @return the PROCESS_STEP
     */
    public String PROCESS_STEP() {
        return PROCESS_STEP;
    }

    /**
     * @return the PROCESSOR
     */
    public String PROCESSOR() {
        return PROCESSOR;
    }

    /**
     * @return the PROCESS_COST
     */
    public String PROCESS_COST() {
        return PROCESS_COST;
    }

    /**
     * @return the UNIT_VALUE
     */
    public String UNIT_VALUE() {
        return UNIT_VALUE;
    }

    /**
     * @return the PROD_VALUE
     */
    public String PROD_VALUE() {
        return PROD_VALUE;
    }

    /**
     * @return the LOW_BOUND
     */
    public String LOW_BOUND() {
        return LOW_BOUND;
    }

    /**
     * @return the UPPER_BOUND
     */
    public String UPPER_BOUND() {
        return UPPER_BOUND;
    }

    /**
     * @return the PLAN_VALUE
     */
    public String PLAN_VALUE() {
        return PLAN_VALUE;
    }

    /**
     * @return the RESOURCE
     */
    public String RESOURCE() {
        return RESOURCE;
    }

    /**
     * @return the RESOURCE_VALUE
     */
    public String RESOURCE_VALUE() {
        return RESOURCE_VALUE;
    }
    
    /**
     * @return the RESOURCE_VALUE
     */
    public String PROD_RESOURCE_LIMIT() {
        return PROD_RESOURCE_LIMIT;
    }

    /**
     * @return the FIXED
     */
    public String FIXED() {
        return FIXED;
    }

    /**
     * 
     * @return 
     */
    public String getFullName() {
        return getProd() + "_" + getProcessStep() + "_" + getResource();
    }
    
    /**
     * 
     * @return 
     */
    public String getProdStepName() {
        return getProd() + "_" + getProcessStep();
    }

    /**
     * @return the measureCoeff
     */
    public int getPrecisionCoeff() {
        return precisionCoeff;
    }

    /**
     * @param measureCoeff the measureCoeff to set
     */
    public void setPrecisionCoeff(int precisionCoeff) {
        this.precisionCoeff = precisionCoeff;
    }
    
    private int getInteger(Object number){
        int retValue = 0;
        
        if(number instanceof Integer){
            retValue = (Integer)number;
        } else if(number instanceof Double){
            double dbl = (Double)number;
            retValue = (int)dbl;
        } else if(number instanceof Float){
            float flt = (Float)number;
            retValue = (int)flt;
        } else if(number instanceof String){
            try{
                retValue = Integer.parseInt((String)number);
            } catch(Exception e){
                
            }
        }
        
        return retValue;
    }
}
