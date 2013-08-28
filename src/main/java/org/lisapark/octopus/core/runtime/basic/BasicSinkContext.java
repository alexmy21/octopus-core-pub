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

import org.lisapark.octopus.core.runtime.SinkContext;

import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class BasicSinkContext implements SinkContext {

    private final PrintStream standardOut;
    private final PrintStream standardError;

    public BasicSinkContext(PrintStream standardOut, PrintStream standardError) {
        checkArgument(standardOut != null, "standardOut cannot be null");
        checkArgument(standardError != null, "standardError cannot be null");
        this.standardOut = standardOut;
        this.standardError = standardError;
    }

    @Override
    public PrintStream getStandardOut() {
        return standardOut;
    }

    @Override
    public PrintStream getStandardError() {
        return standardError;
    }
}
