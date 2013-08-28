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
package org.lisapark.octopus.util;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public abstract class Booleans {
    public static boolean parseBoolean(String value) {
        return parseBoolean(value, false);
    }

    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        return parseBoolean(value, Boolean.valueOf(defaultValue));
    }

    public static Boolean parseBoolean(String value, Boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        String lowerCasedValue = value.toLowerCase();

        return (lowerCasedValue.equals("true") || lowerCasedValue.equals("1") ||
                lowerCasedValue.equals("on") || lowerCasedValue.equals("yes") || lowerCasedValue.equals("y"));
    }
}