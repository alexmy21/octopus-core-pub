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

import com.google.common.base.Objects;

/**
 * Data structure for holding a pair of objects.
 *
 * @author dave sinclair (dsinclair@chariotsolutions.com)
 * @param <F> type of First object of Pair
 * @param <S> type if Second object of Pair
 */
public class Pair<F, S> {

    private final F first;
    private final S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object rhs) {
        if (this == rhs) {
            return true;
        }
        if (rhs == null || getClass() != rhs.getClass()) {
            return false;
        }

        Pair otherPair = (Pair) rhs;

        if (first != null && otherPair.first != null && (!first.equals(otherPair.first))) {
            return false;
        }

        return second != null && otherPair.second != null && second.equals(otherPair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("first", first).
                add("second", second).toString();
    }

    public static <F, S> Pair<F, S> newInstance(F first, S second) {
        return new Pair<F, S>(first, second);
    }
}
