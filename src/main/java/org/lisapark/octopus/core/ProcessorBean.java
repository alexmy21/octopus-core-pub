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
package org.lisapark.octopus.core;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 *
 * @author Alex
 */
public class ProcessorBean {
    
    private final static java.util.logging.Logger logger 
            = java.util.logging.Logger.getLogger(ProcessorBean.class.getName());
    
    public static final String PROC_ID      = "procId";
    public static final String CLASS_NAME   = "className";
    public static final String NAME         = "name";
    public static final String PROC_TYPE    = "procType";
    public static final String AUTHOR_EMAIL = "authorEmail";
    public static final String DESCRIPTION  = "description";
    public static final String PROC_JSON    = "procJson";
    
    private String procId;
    private String className;
    private String name;
    private String procType;
    private String authorEmail;
    private String description;
    
    private Map<String, Object> params;
    
    /**
     * @return the procId
     */
    public String getProcId() {
        return procId;
    }

    /**
     * @param procId the procId to set
     */
    public void setProcId(String procId) {
        this.procId = procId;
    }

    /**
     * @return the className
     */
    public String getClassName() {
        return className;
    }

    /**
     * @param className the className to set
     */
    public void setClassName(String className) {
        this.className = className;
    }

    /**
     * @return the procName
     */
    public String getName() {
        return name == null ? "" : name;
    }

    /**
     * @param procName the procName to set
     */
    public void setName(String procName) {
        this.name = procName;
    }

    /**
     * @return the procType
     */
    public String getProcType() {
        return procType;
    }

    /**
     * @param procType the procType to set
     */
    public void setProcType(String procType) {
        this.procType = procType;
    }

    /**
     * @return the params
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * @param params the params to set
     */
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    /**
     * @return the authorEmail
     */
    public String getAuthorEmail() {
        return authorEmail == null ? "" : authorEmail;
    }

    /**
     * @param authorEmail the authorEmail to set
     */
    public void setAuthorEmail(String authorEmail) {
        this.authorEmail = authorEmail;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description == null ? "" : description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
    
    /** Tolerance value is in the range [0 .. 1], 
     *  0 - no difference, both maps are
     *      the same;
     *  1 - totally different.
     * 
     * Processors with a different ClassNames are considered incomparable with
     * tolerance = null.
     */
    
    /**
     * 
     * @param bean
     * @param threshold
     * @return 
     */
    public Double compare(ProcessorBean bean){
        Double tolerance = null;
        
        if (this.getClassName().equalsIgnoreCase(bean.getClassName())) {
            Map<String, Object> thisParams = Maps.newHashMap(this.getParams());
            Map<String, Object> thatParams = Maps.newHashMap(bean.getParams());
            
            MapDifference<String, Object> diff = Maps.difference(thisParams, thatParams);
            
            int different   = diff.entriesDiffering().size();
            int inCommon    = diff.entriesInCommon().size();
            
            tolerance = ((double)different /(double)(thisParams.size() + thatParams.size() - inCommon));
        }
        
        return tolerance;
    }

}
