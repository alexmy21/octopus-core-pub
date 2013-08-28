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
package org.lisapark.octopus.core.sink.external;

import com.google.common.collect.ImmutableList;
import org.lisapark.octopus.core.Input;
import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.runtime.SinkContext;
import org.lisapark.octopus.core.sink.Sink;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public abstract class CompiledExternalSink {
    private final List<? extends Input> inputs;
    private final UUID id;

    protected CompiledExternalSink(Sink sink) {
        this.id = sink.getId();
        this.inputs = sink.getInputs();
    }

    public UUID getId() {
        return id;
    }

    public List<? extends Input> getInputs() {
        return ImmutableList.copyOf(inputs);
    }

    public abstract void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId);
}
