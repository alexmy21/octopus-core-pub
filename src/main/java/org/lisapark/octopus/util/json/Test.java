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
package org.lisapark.octopus.util.json;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class Test {
    
    public static void main(String[] args){
        String date = convertDate("09.01.2009");
        System.out.println(date);
    }
    
    private static String convertDate(String string) {
            String istring = string.replace('.', '-');
            String[] dateparts = istring.split("-");
            
//            logger.log(Level.INFO, "convertDate: ====> {0}", istring);
            
            if(dateparts.length == 3){
                return dateparts[2] + "-" + dateparts[1] + "-" + dateparts[0];
            } else {
                return "2012" + "-" + "06" + "-" + "13";
            }
        }
    
}
