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
package org.lisapark.octopus.core.processor;

import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.source.Source;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class ProcessorInput<T> extends Input<T> {

    private Attribute sourceAttribute;

    private ProcessorInput(Builder<T> builder) {
        super(builder.id, builder.name, builder.description, builder.type);
    }

    private ProcessorInput(ProcessorInput<T> copyFromInput) {
        super(copyFromInput);

        if (copyFromInput.sourceAttribute != null) {
            this.sourceAttribute = (copyFromInput.sourceAttribute == null) ? null : copyFromInput.sourceAttribute.copyOf();
        }
    }

    public Attribute getSourceAttribute() {
        return sourceAttribute;
    }

    public String getSourceAttributeName() {
        String name = null;

        if (sourceAttribute != null) {
            name = sourceAttribute.getName();
        }

        return name;
    }

    public boolean isConnectedTo(Source source, Attribute attribute) {
        boolean isConnected = super.isConnectedTo(source);

        if (isConnected) {
            isConnected = (sourceAttribute != null && sourceAttribute.equals(attribute));
        }

        return isConnected;
    }

    @Override
    public ProcessorInput<T> clearSource() {
        super.clearSource();
        this.sourceAttribute = null;

        return this;
    }

    @Override
    public ProcessorInput<T> connectSource(Source source) {
        return (ProcessorInput<T>) super.connectSource(source);
    }

    public ProcessorInput<T> clearSourceAttribute() {
        this.sourceAttribute = null;

        return this;
    }

    /**
     * Returns true if the current {@link #getSource()} of this input contains the specified attribute. A source
     * contains an attribute by way of it's {@link org.lisapark.octopus.core.Output#getEventType()} .
     *
     * @param attribute to check
     * @return true if the attribute is from this input's source
     */
    protected boolean isAttributeFromSource(Attribute attribute) {
        checkArgument(attribute != null, "attribute cannot be null");

        return getSource() != null && getSource().getOutput().getEventType().containsAttribute(attribute);
    }

    public void setSourceAttribute(String attributeName) throws ValidationException {
        checkArgument(attributeName != null, "attributeName cannot be null");

        if (this.getSource() == null) {
            throw new ValidationException("Cannot set the source before setting the source attribute");
        }

        Attribute _sourceAttribute = getSource().getOutput().getAttributeByName(attributeName);
        if (_sourceAttribute == null) {
            throw new ValidationException(String.format("Source does not contain an attribute named '%s'", attributeName));
        }
        if (!_sourceAttribute.isCompatibleWith(getType())) {
            throw new ValidationException(
                    String.format("The attribute '%s' of type '%s' is not compatible with the input '%s' of type %s",
                            _sourceAttribute, _sourceAttribute.getType().getSimpleName(),
                            getName(), getType().getSimpleName())
            );
        }
        this.sourceAttribute = _sourceAttribute;
    }

    public void setSourceAttribute(Attribute sourceAttribute) throws ValidationException {
        checkArgument(sourceAttribute != null, "sourceAttribute cannot be null");

        if (this.getSource() == null) {
            throw new ValidationException("Cannot set the source before setting the source attribute");
        }

        String attributeName = sourceAttribute.getName();
        if (getSource().getOutput().getAttributeByName(attributeName) == null) {
            throw new ValidationException(String.format("Source does not contain an attribute named '%s'", attributeName));
        }

        // todo remove duplicate code
        if (!sourceAttribute.isCompatibleWith(getType())) {
            throw new ValidationException(
                    String.format("The attribute '%s' of type '%s' is not compatible with the input '%s' of type %s",
                            attributeName, sourceAttribute.getType().getSimpleName(),
                            getName(), getType().getSimpleName())
            );
        }

        this.sourceAttribute = sourceAttribute;
    }

    @Override
    public ProcessorInput<T> copyOf() {
        return new ProcessorInput<T>(this);
    }

    /**
     * Validates the state of this input to ensure that both the {@link #source} and {@link #sourceAttribute} are
     * non-null.
     *
     * @throws org.lisapark.octopus.core.ValidationException
     *          thrown if there is a problem
     */
    @Override
    public void validate() throws ValidationException {
        super.validate();

        if (this.sourceAttribute == null) {
            throw new ValidationException(String.format("Please set the source attribute for input '%s'", getName()));
        }
    }

    public static Builder<String> stringInputWithId(int id) {
        return new Builder<String>(id, String.class);
    }

    public static Builder<Boolean> booleanInputWithId(int id) {
        return new Builder<Boolean>(id, Boolean.class);
    }

    public static Builder<Long> longInputWithId(int id) {
        return new Builder<Long>(id, Long.class);
    }

    public static Builder<Short> shortInputWithId(int id) {
        return new Builder<Short>(id, Short.class);
    }

    public static Builder<Integer> integerInputWithId(int id) {
        return new Builder<Integer>(id, Integer.class);
    }

    public static Builder<Double> doubleInputWithId(int id) {
        return new Builder<Double>(id, Double.class);
    }

    public static Builder<Float> floatInputWithId(int id) {
        return new Builder<Float>(id, Float.class);
    }

    public static Builder<Object> objectInputWithId(int id) {
        return new Builder<Object>(id, Object.class);
    }
    
    public static class Builder<T> {
        private String name;
        private String description;
        private final int id;
        private final Class<T> type;

        private Builder(int id, Class<T> type) {
            this.id = id;
            this.type = type;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        public ProcessorInput<T> build() {
            return new ProcessorInput<T>(this);
        }
    }
}
