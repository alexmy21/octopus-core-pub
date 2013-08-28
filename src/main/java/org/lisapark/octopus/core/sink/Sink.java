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
package org.lisapark.octopus.core.sink;

import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.Node;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.source.Source;

import java.util.List;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public interface Sink extends Node {

    List<? extends Input> getInputs();

    boolean isConnectedTo(Source source);

    void disconnect(Source source);
}
