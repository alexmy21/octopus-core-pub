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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.awt.*;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import javax.swing.*;
import org.lisapark.octopus.core.parameter.Parameter;
import org.lisapark.octopus.core.sink.external.ExternalSink;
import org.lisapark.octopus.core.source.external.ExternalSource;

/**
 * Abstract Base class implementation of the {@link Node} interface that contains a {@link #name}, {@link #description}
 * and {@link #location} along with the corresponding setter/getter implementations from the {@link Node} interface.
 * <p/>
 * This class also provides method to manipulate the {@link Parameter}s for this node.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public abstract class AbstractNode implements Node {

    private final UUID  id;
    private String      name;
    private String      authorEmail;
    private String      description;
    private Point       location;
    private Icon        icon;
    
    private Set<Parameter> parameters = Sets.newHashSet();

    protected AbstractNode(UUID id) {
        this.id = id;
    }

    protected AbstractNode(UUID id, String name, String description) {
        this.id = id;
        setName(name);
        setDescription(description);
    }

    protected AbstractNode(UUID id, AbstractNode copyFromNode) {
        this.id = id;
        setName(copyFromNode.name);
        setDescription(copyFromNode.description);
        for (Parameter parameter : copyFromNode.getParameters()) {
            this.addParameter(parameter.copyOf());
        }
    }

    protected AbstractNode(AbstractNode copyFromNode) {
        this.id = copyFromNode.id;
        setName(copyFromNode.name);
        setDescription(copyFromNode.description);

        for (Parameter parameter : copyFromNode.getParameters()) {
            this.addParameter(parameter.copyOf());
        }
    }

    @Override
    public final UUID getId() {
        return id;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public AbstractNode setName(String name) {
        checkArgument(name != null, "name cannot be null");
        this.name = name;

        return this;
    }

    @Override
    public final String getDescription() {
        return description;
    }

    @Override
    public AbstractNode setDescription(String description) {
        checkArgument(description != null, "description cannot be null");
        this.description = description;

        return this;
    }

    @Override
    public Point getLocation() {
        return location;
    }

    @Override
    public AbstractNode setLocation(Point location) {
        checkArgument(location != null, "location cannot be null");
        this.location = location;

        return this;
    }

    @Override
    public Icon getIcon() {
        return icon;
    }

    @Override
    public Node setIcon(Icon icon) {
        checkArgument(icon != null, "Icon cannot be null");
        this.icon = icon;

        return this;
    }

    protected void addParameter(Parameter parameter) {
        this.parameters.add(parameter);
    }

    protected void addParameter(Parameter.Builder parameter) {
        this.parameters.add(parameter.build());
    }

    protected Parameter getParameter(int parameterId) {
        return AbstractComponent.getComponentById(parameters, parameterId);
    }

    protected String getParameterValueAsString(int parameterId) {
        return AbstractComponent.getComponentById(parameters, parameterId).getValueAsString();
    }

    @Override
    public Set<Parameter> getParameters() {
        return ImmutableSet.copyOf(this.parameters);
    }

    /**
     * This node will check the validity of it's {@link #parameters}
     *
     * @throws ValidationException if a parameter is not valid
     */
    @Override
    public void validate() throws ValidationException {
        for (Parameter parameter : parameters) {
            parameter.validate();
        }
    }

    /**
     * This implementation of equals will check that the specified otherObject is an instance of a {@link Node}
     * and the {@link #getId()} are equivalent.
     *
     * @param otherObject the reference object with which to compare.
     * @return <code>true</code> if this object is the same as the otherObject; <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof Node)) {
            return false;
        }

        AbstractNode that = (AbstractNode) otherObject;

        return this.getId().equals(that.getId());
    }

    /**
     * This implementation of hashCode returns the {@link java.util.UUID#hashCode()} of the {@link #getId()}
     *
     * @return a hash code value for this node.
     */
    @Override
    public int hashCode() {
        return getId().hashCode();
    }
    
    @Override
    public String toJson(){
        
        ProcessorBean procBean = new ProcessorBean();
        
        procBean.setClassName(getClass().getCanonicalName());
        procBean.setProcId(getId().toString());
        procBean.setName(getName());
        procBean.setAuthorEmail(getAuthorEmail());
        procBean.setDescription(getDescription());
        
        String type;
        if (this instanceof ExternalSink) {
            type = JsonUtils.SINK;
        } else if (this instanceof ExternalSource) {
            type = JsonUtils.SOURCE;
        } else {
            type = JsonUtils.PROCESSOR;
        }        
        
        procBean.setProcType(type);
        
        Set<Parameter> params = getParameters();
        HashMap<String, Object> map = Maps.newHashMap();
        
        for (Parameter param : params) {
            map.put(JsonUtils.formatString(param.getName()), param.getValue());
        }
        
        procBean.setParams(map);
                        
        String jsonString = new Gson().toJson(procBean, ProcessorBean.class);

        return jsonString;
    }

    /**
     * @return the authorEmail
     */
    public String getAuthorEmail() {
        return authorEmail;
    }

    /**
     * @param authorEmail the authorEmail to set
     */
    public void setAuthorEmail(String authorEmail) {
        this.authorEmail = authorEmail;
    }
}
