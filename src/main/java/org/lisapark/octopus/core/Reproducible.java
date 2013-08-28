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
 * A {@link Reproducible} is capable of creating new instance based on itself, i.e. a copy of itself but with a new
 * identity. This means that after calling {@link #newInstance()} on object a, the call
 * <code>a.equals(a.newInstance())</code> should return false. This differs from {@link Copyable} where the objects
 * should be equivalent.
 * <p/>
 * Shallow copying versus deep copying is up to the implementer, but we are generally making deeps copies unless the
 * object is immutable.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public interface Reproducible {

    /**
     * Implementers need to return a new instance based on <code>this</code> instance.
     *
     * @return new instance
     */
    Reproducible newInstance();
}
