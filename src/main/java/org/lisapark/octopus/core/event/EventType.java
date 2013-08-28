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
package org.lisapark.octopus.core.event;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.lisapark.octopus.core.Copyable;
import org.lisapark.octopus.core.Persistable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An {@link EventType} is the definition of an {@link Event} that describes some or all of the attributes a event
 * will have.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class EventType implements Copyable {

    private final List<Attribute> attributes = Lists.newArrayList();

    public EventType() {
    }

    private EventType(EventType copyFromEventType) {
        for (Attribute copyFromAttribute : copyFromEventType.attributes) {
            this.attributes.add(copyFromAttribute.copyOf());
        }
    }

    public int getNumberOfAttributes() {
        return attributes.size();
    }

    public Attribute getAttributeAt(int index) {
        return attributes.get(index);
    }

    public void removeAttributeAt(int index) {
        checkArgument(index > -1 && index < attributes.size(), "index is out of range");
        attributes.remove(index);
    }

    public int indexOfAttribute(Attribute attribute) {
        checkArgument(attribute != null, "attribute cannot be null");
        int index = -1;

        for (int i = 0; i < attributes.size(); ++i) {
            Attribute candidateAttribute = attributes.get(i);

            if (candidateAttribute.equals(attribute)) {
                index = i;
                break;
            }
        }

        return index;
    }

    public boolean containsAttributeWithName(String attributeName) {
        return getAttributeByName(attributeName) != null;
    }

    public EventType addAttribute(Attribute attribute) {
        attributes.add(attribute);

        return this;
    }

    public EventType removeAllAttributes() {
        attributes.clear();

        return this;
    }

    public EventType removeAttribute(Attribute attribute) {
        attributes.remove(attribute);

        return this;
    }

    public Attribute getAttributeByName(String name) {
        Attribute attr = null;

        for (Attribute candidateAttr : attributes) {
            if (name.equals(candidateAttr.getName())) {
                attr = candidateAttr;
                break;
            }
        }

        return attr;
    }

    public boolean containsAttribute(Attribute attribute) {
        return attributes.contains(attribute);
    }

    public List<Attribute> getAttributes() {
        return ImmutableList.copyOf(attributes);
    }

    public Map<String, Object> getEventDefinition() {
        Map<String, Object> definition = Maps.newHashMap();

        for (Attribute attribute : attributes) {
            definition.put(attribute.getName(), attribute.getType());
        }

        return definition;
    }

    public Collection<String> getAttributeNames() {
        Collection<String> attributeNames = Lists.newLinkedList();
        for (Attribute attribute : attributes) {
            attributeNames.add(attribute.getName());
        }
        return attributeNames;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("attributes", attributes).toString();
    }

    @Override
    public EventType copyOf() {
        return new EventType(this);
    }
}
