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

import org.lisapark.octopus.core.event.Attribute;
import org.lisapark.octopus.core.event.EventType;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class Output extends AbstractComponent {

    private EventType eventType;

    protected Output(int id) {
        super(id);
        this.eventType = new EventType();
    }

    protected Output(int id, EventType eventType) {
        super(id);
        this.eventType = eventType;
    }

    protected Output(int id, String name, String description) {
        super(id, name, description);
        this.eventType = new EventType();
    }

    protected Output(Output existingOutput) {
        super(existingOutput);
        this.eventType = existingOutput.eventType.copyOf();
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public EventType getEventType() {
        return eventType;
    }

    @Override
    public Output copyOf() {
        return new Output(this);
    }

    @Override
    public Output setDescription(String description) {
        return (Output) super.setDescription(description);
    }

    @Override
    public Output setName(String name) {
        return (Output) super.setName(name);
    }

    public EventType addAttribute(Attribute attribute) {
        return eventType.addAttribute(attribute);
    }

    public EventType removeAttribute(Attribute attribute) {
        return eventType.removeAttribute(attribute);
    }

    public boolean containsAttribute(Attribute attribute) {
        return eventType.containsAttribute(attribute);
    }

    public Attribute getAttributeByName(String name) {
        return eventType.getAttributeByName(name);
    }

    public Map<String, Object> getEventDefinition() {
        return eventType.getEventDefinition();
    }

    public List<Attribute> getAttributes() {
        return eventType.getAttributes();
    }

    public Collection<String> getAttributeNames() {
        return eventType.getAttributeNames();
    }

    public static Output outputWithId(int id) {
        return new Output(id);
    }

    public static Output outputWithIdAndEventType(int id, EventType eventType) {
        return new Output(id, eventType);
    }
}
