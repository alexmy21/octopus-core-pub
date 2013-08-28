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

import com.google.common.collect.Maps;
import org.lisapark.octopus.core.Persistable;

import java.util.Collection;
import java.util.Map;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class Event {
    private final Map<String, Object> data = Maps.newHashMap();

    public Event(String attributeName, Object value) {
        data.put(attributeName, value);
    }

    public Event(Map<String, Object> data) {
        this.data.putAll(data);
    }

    public Event unionWith(Event event) {
        Map<String, Object> newData = Maps.newHashMap(data);
        newData.putAll(event.getData());

        return new Event(newData);
    }

    public Event unionWith(Collection<Event> events) {
        Map<String, Object> newData = Maps.newHashMap(data);
        for (Event event : events) {
            newData.putAll(event.getData());
        }

        return new Event(newData);
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Integer getAttributeAsInteger(String attributeName) {
        Object value = data.get(attributeName);

        if (value != null) {
            return ((Number) value).intValue();
        } else {
            return null;
        }
    }

    public Short getAttributeAsShort(String attributeName) {
        Object value = data.get(attributeName);

        if (value != null) {
            return ((Number) value).shortValue();
        } else {
            return null;
        }
    }

    public Long getAttributeAsLong(String attributeName) {
        Object value = data.get(attributeName);

        if (value != null) {
            return ((Number) value).longValue();
        } else {
            return null;
        }
    }

    public Float getAttributeAsFloat(String attributeName) {
        Object value = data.get(attributeName);

        if (value != null) {
            return ((Number) value).floatValue();
        } else {
            return null;
        }
    }

    public Double getAttributeAsDouble(String attributeName) {
        Object value = data.get(attributeName);

        if (value != null) {
            return ((Number) value).doubleValue();
        } else {
            return null;
        }
    }

    public String getAttributeAsString(String attributeName) {
        return (String) data.get(attributeName);
    }

    public Boolean getAttributeAsBoolean(String attributeName) {
        return (Boolean) data.get(attributeName);
    }

    @Override
    public String toString() {
        return "Event{" +
                "data=" + data +
                '}';
    }
}
