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
public class Product {
    
    public static final String PRODUCT_NAME         = "productName";
    public static final String PRODUCT_PRICE        = "productPrice";
    public static final String PRODUCT_ID           = "productId";
    public static final String PRODUCTION_LOW       = "productionLowBound";
    public static final String PRODUCTION_UPPER     = "productionUpperBound";
    public static final String PRODUCTION_VALUE     = "productionValue";
    public static final String DESCRIPTION          = "description";
    public static final String PRODUCT_JSON         = "productJson";
    
    private String      productId;
    private String      productName;
    private String      description;
    private Integer     productPrice;
    
    private Integer     productionValue;
    private Integer     productionLowBound;
    private Integer     productionUpperBound;

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
     * @return the productName
     */
    public String getProductName() {
        return productName;
    }

    /**
     * @param productName the productName to set
     */
    public void setProductName(String productName) {
        this.productName = productName;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the productPrice
     */
    public Integer getProductPrice() {
        return productPrice;
    }

    /**
     * @param productPrice the productPrice to set
     */
    public void setProductPrice(Integer productPrice) {
        this.productPrice = productPrice;
    }

    /**
     * @return the productionValue
     */
    public Integer getProductionValue() {
        return productionValue;
    }

    /**
     * @param productionValue the productionValue to set
     */
    public void setProductionValue(Integer productionValue) {
        this.productionValue = productionValue;
    }

    /**
     * @return the productionLowBound
     */
    public Integer getProductionLowBound() {
        return productionLowBound;
    }

    /**
     * @param productionLowBound the productionLowBound to set
     */
    public void setProductionLowBound(Integer productionLowBound) {
        this.productionLowBound = productionLowBound;
    }

    /**
     * @return the productionUpperBound
     */
    public Integer getProductionUpperBound() {
        return productionUpperBound;
    }

    /**
     * @param productionUpperBound the productionUpperBound to set
     */
    public void setProductionUpperBound(Integer productionUpperBound) {
        this.productionUpperBound = productionUpperBound;
    }

}
