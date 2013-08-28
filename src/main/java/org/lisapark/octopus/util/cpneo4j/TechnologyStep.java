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

import java.util.Set;

/**
 *
 * @author Alex
 */
public class TechnologyStep {
    
    public static final String TECHNOLOGY_STEP_ID       = "technologyStepId";
    public static final String TECHNOLOGY_STEP_NAME     = "technologyStepName";
    public static final String DESCRIPTION              = "description";
    public static final String STEP_STATUS              = "stepStatus";
    public static final String TECH_STEP_NUMBER         = "techStepNumber";
    public static final String RESOURCE_REQUIRED        = "resourceRequired";
    public static final String PRODUCTION               = "production";
    public static final String STEPS_REQUIRED           = "stepsRequired";
    
    public static final String TECHNOLOGY_STEP_JSON     = "technologyStepJson";
    
    private String      technologyStepId;
    private String      technologyStepName;
    private String      description;
    
    private String      productId;    
    private String      machineResourceId; 
    
    private Integer     stepStatus;   // 0 - available to use;  1 - fixed; 
    
    private Integer     techStepNumber;
    
    private Integer     resourceRequired; 
    private Integer     production;
    
    private Set<String> stepsRequired;

    /**
     * @return the technologyStepId
     */
    public String           getTechnologyStepId() {
        return technologyStepId;
    }

    /**
     * @param technologyStepId the technologyStepId to set
     */
    public void             setTechnologyStepId(String technologyStepId) {
        this.technologyStepId = technologyStepId;
    }

    /**
     * @return the technologyStepName
     */
    public String           getTechnologyStepName() {
        return technologyStepName;
    }

    /**
     * @param technologyStepName the technologyStepName to set
     */
    public void             setTechnologyStepName(String technologyStepName) {
        this.technologyStepName = technologyStepName;
    }

    /**
     * @return the description
     */
    public String           getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void             setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the technologyStepStatus
     */
    public Integer          getStepStatus() {
        return stepStatus;
    }

    /**
     * @param technologyStepStatus the technologyStepStatus to set
     */
    public void             setStepStatus(Integer technologyStepStatus) {
        this.stepStatus = technologyStepStatus;
    }

    /**
     * @return the resourcePerProduct
     */
    public Integer          getResourceRequired() {
        return resourceRequired;
    }

    /**
     * @param resourcePerProduct the resourcePerProduct to set
     */
    public void             setResourceRequired(Integer resourcePerProduct) {
        this.resourceRequired = resourcePerProduct;
    }

    /**
     * @return the production
     */
    public Integer          getProduction() {
        return production;
    }

    /**
     * @param production the production to set
     */
    public void             setProduction(Integer production) {
        this.production = production;
    }

    /**
     * @return the stepsRequired
     */
    public Set<String> getStepsRequired() {
        return stepsRequired;
    }

    /**
     * @param stepsRequired the stepsRequired to set
     */
    public void setStepsRequired(Set<String> stepsRequired) {
        this.stepsRequired = stepsRequired;
    }

    /**
     * @return the productId
     */
    public String getProductId() {
        return productId;
    }

    /**
     * @param productId the productId to set
     */
    public void setProductId(String productId) {
        this.productId = productId;
    }

    /**
     * @return the machineResourceId
     */
    public String getMachineResourceId() {
        return machineResourceId;
    }

    /**
     * @param machineResourceId the machineResourceId to set
     */
    public void setMachineResourceId(String machineResourceId) {
        this.machineResourceId = machineResourceId;
    }

    /**
     * @return the techStepNumber
     */
    public Integer getTechStepNumber() {
        return techStepNumber;
    }

    /**
     * @param techStepNumber the techStepNumber to set
     */
    public void setTechStepNumber(Integer techStepNumber) {
        this.techStepNumber = techStepNumber;
    }
    
    
}
