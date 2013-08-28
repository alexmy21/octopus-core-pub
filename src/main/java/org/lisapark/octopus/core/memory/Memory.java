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
package org.lisapark.octopus.core.memory;

import org.lisapark.octopus.core.event.Event;

import java.util.Collection;

/**
 * A {@link Memory} is used to store {@link Event}s for a {@link org.lisapark.octopus.core.processor.Processor}.
 * <p/>
 * Some {@link org.lisapark.octopus.core.processor.Processor}s required memory to store computed or temporary values
 * in order to compute a computation.  An example of this is the {@link org.lisapark.octopus.core.processor.Sma} that
 * contains a window length which is a number of {@link Event}s for which the average should be computed over.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public interface Memory<T> {

    void add(T value);

    boolean remove(T value);

    Collection<T> values();
}
