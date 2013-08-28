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
package org.lisapark.octopus.repository.db4o;

import com.db4o.instrumentation.core.ClassFilter;
import org.lisapark.octopus.core.Persistable;

import java.lang.annotation.Annotation;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
public class PersistableFilter implements ClassFilter {
    @Override
    public boolean accept(Class<?> aClass) {
        if (null == aClass || aClass.equals(Object.class)) {
            return false;
        }
        return hasAnnotation(aClass)
                || accept(aClass.getSuperclass());
    }

    private boolean hasAnnotation(Class<?> aClass) {
        // We compare by name, to be class-loader independent
        Annotation[] annotations = aClass.getAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().getName()
                    .equals(Persistable.class.getName())) {
                return true;
            }
        }
        return false;
    }

}
