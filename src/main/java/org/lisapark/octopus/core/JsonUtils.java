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

/**
 *
 * @author Alex
 */
public class JsonUtils {    
    public static String PROC_NAME  = "procName";
    public static String PROC_ID    = "procId";
    public static String PROC_TYPE  = "procType";
    public static String PROCESSOR  = "processor";
    public static String SINK       = "sink";
    public static String SOURCE     = "source";
    public static String PARAMS     = "params";
    public static String SOURCES    = "sources";
    public static String PROCESSORS = "processors";
    public static String SINKS      = "sinks";
    public static String CLASS_NAME = "className";
    
    public static String quotedString(String string) {
        return "\"" + string + "\""; 
    }
    
    public static String formatString(String string){
        String stringOut = string.trim().toLowerCase();
        StringBuilder builder = new StringBuilder();
        
        for(char c : stringOut.toCharArray()){
            if(c >= 97 && c <= 122 || c >= 48 && c <= 57){
                builder.append(c);
            } else if(c == 32){
                builder.append('_');
            }
        }
        return builder.toString();
    }
}
