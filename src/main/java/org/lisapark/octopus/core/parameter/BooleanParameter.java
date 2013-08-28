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
public class BooleanParameter extends Parameter<Boolean> {

    protected BooleanParameter(Builder<Boolean> builder) {
        super(builder);
    }

    protected BooleanParameter(BooleanParameter existingParameter) {
        super(existingParameter);
    }

    @Override
    public Class<Boolean> getType() {
        return Boolean.class;
    }

    @Override
    public String getValueForDisplay() {
        return String.valueOf(getValue());
    }

    @Override
    public Boolean parseValueFromString(String stringValue) throws ConversionException {
        return parseBoolean(stringValue);
    }

    @Override
    public Parameter<Boolean> copyOf() {
        return new BooleanParameter(this);
    }

    static Boolean parseBoolean(String value) throws ConversionException {
        String lowerCasedValue = value.toLowerCase();
        Boolean parsedValue = null;

        if (lowerCasedValue.equals("true") || lowerCasedValue.equals("1") ||
                lowerCasedValue.equals("on") || lowerCasedValue.equals("yes") || lowerCasedValue.equals("y")) {
            parsedValue = true;

        } else if (lowerCasedValue.equals("false") || lowerCasedValue.equals("0") ||
                lowerCasedValue.equals("off") || lowerCasedValue.equals("no") || lowerCasedValue.equals("n")) {

            parsedValue = false;
        }

        if (parsedValue == null) {
            throw new ConversionException(String.format("%s is not a valid Boolean value", value));
        }
        return parsedValue;
    }
}
