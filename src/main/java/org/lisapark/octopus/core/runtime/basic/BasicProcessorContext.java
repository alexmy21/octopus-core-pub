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
package org.lisapark.octopus.core.runtime.basic;

import org.lisapark.octopus.core.memory.Memory;
import org.lisapark.octopus.core.runtime.ProcessorContext;

import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class BasicProcessorContext<MEMORY_TYPE> extends BasicSinkContext implements ProcessorContext<MEMORY_TYPE> {
    private final Memory<MEMORY_TYPE> memory;

    public BasicProcessorContext(PrintStream standardOut, PrintStream standardError) {
        super(standardOut, standardError);
        memory = null;
    }

    public BasicProcessorContext(PrintStream standardOut, PrintStream standardError, Memory<MEMORY_TYPE> memory) {
        super(standardOut, standardError);
        checkArgument(memory != null, "memory cannot be null");
        this.memory = memory;
    }

    @Override
    public Memory<MEMORY_TYPE> getProcessorMemory() {
        return memory;
    }
}
