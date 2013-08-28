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
package org.lisapark.octopus.core.memory.heap;

import com.google.common.collect.Lists;
import org.lisapark.octopus.core.memory.Memory;

import java.util.Collection;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class HeapCircularBuffer<T> implements Memory<T> {

    private final T[] buffer;

    private int currentIndex;

    @SuppressWarnings("unchecked")
    public HeapCircularBuffer(int n) {
        buffer = (T[]) new Object[n];
        currentIndex = 0;
    }

    @Override
    public void add(T value) {
        buffer[currentIndex] = value;

        currentIndex = (currentIndex + 1) % buffer.length;
    }

    @Override
    public boolean remove(T value) {
        throw new UnsupportedOperationException("Remove not supported");
    }

    @Override
    public Collection<T> values() {
        Collection<T> values = Lists.newArrayListWithCapacity(buffer.length);

        for (T item : buffer) {
            if (item != null) {
                values.add(item);
            }
        }

        return values;
    }
}
