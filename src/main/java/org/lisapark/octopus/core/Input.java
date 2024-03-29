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

import org.lisapark.octopus.core.event.Event;
import org.lisapark.octopus.core.source.Source;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class Input<T> extends AbstractComponent {

    private final Class<T> type;

    private Source source;

    protected Input(int id, Class<T> type) {
        super(id);
        this.type = type;
    }

    protected Input(int id, String name, String description, Class<T> type) {
        super(id, name, description);
        this.type = type;
    }

    protected Input(Input<T> copyFromInput) {
        super(copyFromInput);
        this.type = copyFromInput.type;

        if (copyFromInput.source != null) {
            this.source = (Source) copyFromInput.source.copyOf();
        }
    }

    public Class<?> getType() {
        return type;
    }

    public Source getSource() {
        return source;
    }

    public boolean isConnected() {
        return source != null;
    }

    public boolean isConnectedTo(Source source) {
        return this.source != null && this.source.equals(source);
    }

    public Input<T> clearSource() {
        this.source = null;

        return this;
    }

    public Input<T> connectSource(Source source) {
        checkArgument(source != null, "source cannot be null");
        this.source = source;

        return this;
    }

    @Override
    public Input<T> copyOf() {
        return new Input<T>(this);
    }

    /**
     * Validates the state of this input to ensure that the {@link #source} is non-null.
     *
     * @throws ValidationException thrown if there is a problem
     */
    @Override
    public void validate() throws ValidationException {
        // just need to ensure that an source has been set
        if (this.source == null) {
            throw new ValidationException(String.format("Please set the source for input '%s'", getName()));
        }
    }

    public static Input<Event> eventInputWithId(int id) {
        return new Input<Event>(id, Event.class);
    }
}
