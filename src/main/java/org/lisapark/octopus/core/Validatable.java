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

/**
 * A {@link Validatable} is capable of validating itself to ensure its internal state is valid. If it is <b>not</b>,
 * said object should throw a {@link org.lisapark.octopus.core.ValidationException}
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public interface Validatable {

    /**
     * Implementers validate the correctness of the current state and throw a {@link ValidationException} if they
     * are not.
     *
     * @throws ValidationException thrown if this object is not valid
     */
    void validate() throws ValidationException;
}
