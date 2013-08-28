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
package org.lisapark.octopus.core.compiler;

import org.lisapark.octopus.core.ProcessingModel;
import org.lisapark.octopus.core.ValidationException;
import org.lisapark.octopus.core.memory.MemoryProvider;
import org.lisapark.octopus.core.runtime.ProcessingRuntime;

import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A {@link Compiler} is used to take a {@link ProcessingModel} and create a {@link ProcessingRuntime}. There
 * different implementations of the compiler depending on the underlying complex event processing engine.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 * @see org.lisapark.octopus.core.processor.Processor
 * @see org.lisapark.octopus.core.ProcessingModel
 */
public abstract class Compiler {

    public abstract ProcessingRuntime compile(ProcessingModel model) throws ValidationException;

    public abstract void setMemoryProvider(MemoryProvider memoryProvider);

    public abstract void setStandardOut(PrintStream standardOut);

    public abstract void setStandardError(PrintStream standardErr);

    // todo

    public static Compiler newCompiler(String className) {
        checkArgument(className != null, "className cannot be null");

        try {
            return (Compiler) Class.forName(className).newInstance();
            // todo new exception?
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
