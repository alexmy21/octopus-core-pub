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
public class ProductTechnologyStep {
       
    private String  productId       = "";
    private Integer techStepNumber  = 0;
    
    public ProductTechnologyStep(String productId, Integer techStepNumber){
        this.productId      = productId;
        this.techStepNumber = techStepNumber;
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
  
    @Override
    public boolean equals(Object o) {
        if(o instanceof ProductTechnologyStep){
            ProductTechnologyStep step = (ProductTechnologyStep)o;
            return this.getProductId().equalsIgnoreCase(step.getProductId())
                    && this.techStepNumber == step.getTechStepNumber();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (this.productId + this.techStepNumber).hashCode();
    }

    
}
