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
package org.lisapark.octopus.core.parameter;

import org.lisapark.octopus.core.Persistable;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class FloatParameter extends Parameter<Float> {

    protected FloatParameter(Builder<Float> builder) {
        super(builder);
    }

    protected FloatParameter(FloatParameter existingParameter) {
        super(existingParameter);
    }

    @Override
    public String getValueForDisplay() {
        return String.valueOf(getValue());
    }

    @Override
    public Class<Float> getType() {
        return Float.class;
    }

    @Override
    public Parameter<Float> copyOf() {
        return new FloatParameter(this);
    }

    @Override
    public Float parseValueFromString(String stringValue) throws ConversionException {
        try {
            return Float.parseFloat(stringValue);
        } catch (NumberFormatException e) {
            throw new ConversionException(String.format("Could not convert %s into a number", stringValue));
        }
    }
}
