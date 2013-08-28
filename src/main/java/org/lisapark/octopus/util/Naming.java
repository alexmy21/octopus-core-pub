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

import org.lisapark.octopus.core.ValidationException;

/**
 * This is a utility class that contains various methods dealing with naming of different elements in the
 * Octopus domain.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public abstract class Naming {

    /**
     * Determines if the specified name is permissible as a valid name.
     *
     * @param name     to check
     * @param nameType used when throwing {@link ValidationException} to identify the type of name
     * @throws org.lisapark.octopus.core.ValidationException
     *          if the name is not valid
     */
    public static void checkValidity(String name, String nameType) throws ValidationException {
        if (name == null) {
            throw new ValidationException(String.format("%s cannot be null", nameType));
        }

        if (name.length() == 0) {
            throw new ValidationException(String.format("%s cannot be empty", nameType));
        }

        if (name.indexOf(' ') > -1) {
            throw new ValidationException(String.format("%s cannot have spaces in it", nameType));
        }

        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            throw new ValidationException(
                    String.format("%s cannot start with a '%c'. " +
                            "Names can only begin with a letter, a '$' or an '_'", nameType, name.charAt(0))
            );
        }

        for (int index = 1; index < name.length(); ++index) {
            if (!Character.isJavaIdentifierPart(name.charAt(index))) {
                throw new ValidationException(
                        String.format("%s cannot contain a '%c'", nameType, name.charAt(index))
                );
            }
        }
    }
}
