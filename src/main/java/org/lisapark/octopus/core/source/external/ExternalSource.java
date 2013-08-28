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
package org.lisapark.octopus.core.source.external;

import org.lisapark.octopus.core.AbstractNode;
import org.lisapark.octopus.core.Output;
import org.lisapark.octopus.core.Persistable;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.source.Source;

import java.util.UUID;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public abstract class ExternalSource extends AbstractNode implements Source {

    private Output output;

    protected ExternalSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected ExternalSource(UUID id) {
        super(id);
    }

    protected ExternalSource(UUID id, ExternalSource copyFromNode) {
        super(id, copyFromNode);
        setOutput(copyFromNode.getOutput().copyOf());
    }

    protected ExternalSource(ExternalSource copyFromNode) {
        super(copyFromNode);
        setOutput(copyFromNode.getOutput().copyOf());
    }

    public abstract CompiledExternalSource compile() throws ValidationException;

    @Override
    public abstract Source newInstance();

    @Override
    public abstract Source copyOf();

    @Override
    public Output getOutput() {
        return output;
    }

    protected void setOutput(Output output) {
        this.output = output;
    }
}

