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
package org.lisapark.octopus.core.parameter;

import org.lisapark.octopus.core.Persistable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class Constraints {

    public static Constraint<String> classConstraintWithMessage(String message) {
        checkArgument(message != null, "message cannot be null");
        return new ClassConstraint(message);
    }

    public static Constraint<Integer> integerConstraintWithMinimumAndMessage(Integer minimum, String message) {
        checkArgument(minimum != null, "minimum cannot be null");
        checkArgument(message != null, "message cannot be null");

        return new IntegerConstraint(message, minimum);
    }

    @Persistable
    static class IntegerConstraint implements Constraint<Integer> {

        private Integer min;
        private Integer max;
        private final String message;

        private IntegerConstraint(String message, Integer min) {
            this(message, min, Integer.MAX_VALUE);
        }

        private IntegerConstraint(String message, Integer min, Integer max) {
            this.message = message;
            this.min = min;
            this.max = max;
        }

        @Override
        public void validate(String name, Integer value) throws ConstraintException {
            if (max == null && value < min) {
                throw new ConstraintException(message);
            }

            if (value < min) {
                throw new ConstraintException(message);
            }

            if (max != null && value > max) {
                throw new ConstraintException(message);
            }
        }

        @Override
        public Constraint<Integer> copyOf() {
            return new IntegerConstraint(message, min, max);
        }
    }

    @Persistable
    static class ClassConstraint implements Constraint<String> {

        private final String message;

        public ClassConstraint(String message) {
            this.message = message;
        }

        @Override
        public void validate(String name, String value) throws ConstraintException {
            try {
                Class.forName(value);
            } catch (ClassNotFoundException e) {
                throw new ConstraintException(String.format(message, value), e);
            }
        }

        @Override
        public ClassConstraint copyOf() {
            return new ClassConstraint(message);
        }
    }
}
