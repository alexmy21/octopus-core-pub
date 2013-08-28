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
package org.lisapark.octopus;

/**
 * This is a unique exception in Octopus, it is thrown only when the exception is the cause of a programmer error. Any
 * time this happens means there is a whole in the programmer's logic and needs to be corrected.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class ProgrammerException extends RuntimeException {

    public ProgrammerException(Throwable cause) {
        super(cause);
    }

    public ProgrammerException(String message) {
        super(message);
    }

    public ProgrammerException(String message, Throwable cause) {
        super(message, cause);
    }
}
