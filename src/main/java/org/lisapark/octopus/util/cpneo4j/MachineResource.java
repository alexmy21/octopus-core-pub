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

/**
 *
 * @author Alex
 */
public class MachineResource {
    
    public static final String MACHINE_RESOURCE_ID  = "machineResourceId";
    public static final String MACHINE_NAME         = "machineName";
    public static final String MEASURE_UNIT         = "measureUnit";
    public static final String DESCRIPTION          = "description";
    public static final String RESOURCE_TOTAL       = "resourceTotal";
    public static final String LOW_BOUND            = "lowBound";
    public static final String UPPER_BOUND          = "upperBound";
    public static final String RESOURCE_USED        = "resourceUsed";
    public static final String RESOURCE_UNIT_COST   = "resourceUnitCost";
    
    public static final String MACHINE_JSON         = "machineJson";
    
    private String  machineResourceId;
    private String  machineName;
    private String  measureUnit;
    private String  description;
    
    private Integer resourceTotal;
    private Integer lowBound;
    private Integer upperBound;
    private Integer resourceUsed;
    private Integer resourceUnitCost;
    
    
    /**
     * @return the machineId
     */
    public String   getMachineResourceId() {
        return machineResourceId;
    }

    /**
     * @param machineId the machineId to set
     */
    public void     setMachineResourceId(String machineId) {
        this.machineResourceId = machineId;
    }

    /**
     * @return the machineName
     */
    public String   getMachineName() {
        return machineName;
    }

    /**
     * @param machineName the machineName to set
     */
    public void     setMachineName(String machineName) {
        this.machineName = machineName;
    }

    /**
     * @return the measureUnit
     */
    public String   getMeasureUnit() {
        return measureUnit;
    }

    /**
     * @param measureUnit the measureUnit to set
     */
    public void     setMeasureUnit(String measureUnit) {
        this.measureUnit = measureUnit;
    }

    /**
     * @return the description
     */
    public String   getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void     setDescription(String description) {
        this.description = description;
    }   
    
    /**
     * @return the resourceValue
     */
    public Integer  getResourceTotal() {
        return resourceTotal;
    }

    /**
     * @param resourceValue the resourceValue to set
     */
    public void     setResourceTotal(Integer resourceValue) {
        this.resourceTotal = resourceValue;
    }

    /**
     * @return the lowBound
     */
    public Integer  getLowBound() {
        return lowBound;
    }

    /**
     * @param lowBound the lowBound to set
     */
    public void     setLowBound(Integer lowBound) {
        this.lowBound = lowBound;
    }

    /**
     * @return the upperBound
     */
    public Integer  getUpperBound() {
        return upperBound;
    }

    /**
     * @param upperBound the upperBound to set
     */
    public void     setUpperBound(Integer upperBound) {
        this.upperBound = upperBound;
    }

    /**
     * @return the resourceUsedValue
     */
    public Integer  getResourceUsed() {
        return resourceUsed;
    }

    /**
     * @param resourceUsedValue the resourceUsedValue to set
     */
    public void     setResourceUsed(Integer resourceValue) {
        this.resourceUsed = resourceValue;
    }

    /**
     * @return the resourceUnitCost
     */
    public Integer  getResourceUnitCost() {
        return resourceUnitCost;
    }

    /**
     * @param resourceUnitCost the resourceUnitCost to set
     */
    public void     setResourceUnitCost(Integer resourceUnitCost) {
        this.resourceUnitCost = resourceUnitCost;
    }
    
}
