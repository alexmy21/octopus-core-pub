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
package org.lisapark.octopus.util.xml;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class XmlCleanUp {
    
    public static void main(String[] str){
        System.out.println(clean(DATA.replaceAll("\t", "")));
    }
    
    private static String clean(String string){
        StringBuilder cleanStr = new StringBuilder();
        
        Character lookFor = '<';
        
        for(Character ch : string.toCharArray()){
            if(ch == lookFor){
                lookFor = lookFor == '<' ? '>' : '<'; 
                cleanStr.append(ch);
            } else if(lookFor == '>'){
                cleanStr.append(ch);
            }            
        }
        
        return cleanStr.toString();
    }
    
    private static final String DATA = 
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
             "<Grid>\n"
            + "<Body>\n"
            + "<B>\n"
            + "<I prod=\"1\" machine=\"2\" unitvalue=\"12\" cost=\"10\" value=\"0\"   machine_name=\"machine2\" step=\"1\" prod_name=\"prod1\" fixed=\"0\" />\n"
            + "<I prod=\"1\" machine=\"1\" unitvalue=\"13\" cost=\"11\" value=\"0\"   machine_name=\"machine1\" step=\"2\" prod_name=\"prod1\" fixed=\"0\" />"
            + "<I prod=\"1\" machine=\"3\" unitvalue=\"14\" cost=\"12\" value=\"8\"   machine_name=\"machine3\" step=\"2\" prod_name=\"prod1\" fixed=\"1\" />"
            + "<I prod=\"1\" machine=\"2\" unitvalue=\"11\" cost=\"9\" 	value=\"0\"  machine_name=\"machine2\" step=\"3\" prod_name=\"prod1\" fixed=\"0\" />"
            + "<I prod=\"1\" machine=\"3\" unitvalue=\"14\" cost=\"12\" value=\"0\"   machine_name=\"machine3\" step=\"3\" prod_name=\"prod1\" fixed=\"0\" /> "
            + "<I prod=\"2\" machine=\"2\" unitvalue=\"12\" cost=\"10\" value=\"0\"   machine_name=\"machine2\" step=\"1\" prod_name=\"prod2\" fixed=\"0\" /> "
            + "<I prod=\"2\" machine=\"3\" unitvalue=\"14\" cost=\"12\" value=\"10\"  machine_name=\"machine3\" step=\"1\" prod_name=\"prod2\" fixed=\"1\" /> "
            + "<I prod=\"2\" machine=\"1\" unitvalue=\"14\" cost=\"12\" value=\"0\"   machine_name=\"machine1\" step=\"2\" prod_name=\"prod2\" fixed=\"0\" /> "
            + "<I prod=\"2\" machine=\"3\" unitvalue=\"13\" cost=\"11\" value=\"0\"   machine_name=\"machine3\" step=\"2\" prod_name=\"prod2\" fixed=\"0\" /> "
            + "<I prod=\"3\" machine=\"2\" unitvalue=\"13\" cost=\"11\" value=\"0\"   machine_name=\"machine2\" step=\"1\" prod_name=\"prod3\" fixed=\"0\" /> "
            + "<I prod=\"3\" machine=\"3\" unitvalue=\"12\" cost=\"10\" value=\"0\"   machine_name=\"machine3\" step=\"1\" prod_name=\"prod3\" fixed=\"0\" /> "
            + "<I prod=\"3\" machine=\"1\" unitvalue=\"11\" cost=\"9\" 	value=\"0\"   machine_name=\"machine1\" step=\"2\" prod_name=\"prod3\" fixed=\"0\" />"
            + "<I prod=\"3\" machine=\"2\" unitvalue=\"14\" cost=\"12\" value=\"0\"   machine_name=\"machine2\" step=\"2\" prod_name=\"prod3\" fixed=\"0\" />"
            + "</B>"
            + "</Body>"
            + "</Grid>";
    
}
