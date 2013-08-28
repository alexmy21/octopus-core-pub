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
package org.lisapark.octopus.util.json;

import java.util.List;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public interface JsonOntology {
    
    /**
     * Uses OpenL rules to determine a level of the node with nodeName,
     * based on the current stack of up-level observed nodes.
     * 
     * @param stack
     * @param nodeName
     * @return - next level:
     *      -1 move one level up;
     *      +1 move one level down;
     *       0 stay on the same level;
     *      any other numbers, that are greater than 0 - jump up to
     *      the (stack.size() - (return number)) level or
     *      to the level 0, if (stack.size() - (return number)) < 0.
     */
    public int nextLevel(List<String> stack, String nodeName);
    
}
