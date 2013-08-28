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
package org.lisapark.octopus.util.cpneo4j;

import choco.Choco;
import choco.Options;
import choco.cp.model.CPModel;
import choco.cp.solver.CPSolver;
import choco.cp.solver.search.BranchingFactory;
import choco.cp.solver.search.integer.valselector.RandomIntValSelector;
import choco.kernel.common.logging.ChocoLogging;
import choco.kernel.common.logging.Verbosity;
import choco.kernel.common.util.tools.ArrayUtils;
import choco.kernel.model.constraints.automaton.FA.FiniteAutomaton;
import choco.kernel.model.variables.integer.IntegerExpressionVariable;
import choco.kernel.model.variables.integer.IntegerVariable;
import java.util.Date;

/**
 *
 * @author Alex
 */
public class Test {

    public static void main(String[] args) {
        System.out.println("Test2");
        final int X = 0;
        final int DN = 1;
        final int NN = 2;
        final int SN = 3;
        final String[] shiftNames = {"", "DN", "NN", "SN"};
        final int[] shifts = {DN, NN, SN};
        final int[] shours = {0, 12, 12, 9};
        final String[] resources = {"Sally", "Sue", "Anne", "Mabel", "Joan", "Pam", "Kitty", "Karen", "Tilly", "Jenn", "Kari", "Kathy", "Mona", "Cammi", "Nancy", "Nellie", "Tammy", "Ashley", "Gwen", "Rene", "Bria", "Chuck", "Tom", "Fred"};
        final int numResources = resources.length;
        final int periodLength = 14;
        IntegerVariable[][] roster = new IntegerVariable[periodLength][numResources];
        int[][] costs = new int[periodLength][shiftNames.length];
        CPModel m = new CPModel();
        System.out.println("Number of resources: " + numResources);
        final int numShifts = shiftNames.length - 1;
        IntegerExpressionVariable[] optVals = new IntegerExpressionVariable[periodLength];
        int t = 2;
        for (int idx = 0; idx < periodLength; idx++) {
            for (int j = 0; j < numResources; j++) {
                // Each cell in roster is assigned a shift which is an integer between 0-numShifts
                roster[idx][j] = Choco.makeIntVar(resources[j] + "_" + idx, 0, numShifts, Options.V_BOUND);
                m.addVariable(roster[idx][j]);
            }
            for (int j = 0; j < shours.length; j++) {
                costs[idx][j] = shours[j];
            }
            IntegerVariable DNocc = Choco.makeIntVar("DNocc_" + idx, 3, 5, Options.V_NO_DECISION);
            IntegerVariable NNocc = Choco.makeIntVar("NNocc_" + idx, 4, 5, Options.V_NO_DECISION);
            IntegerVariable SNocc = Choco.makeIntVar("SNocc_" + idx, 3, 5, Options.V_NO_DECISION);
            IntegerVariable[] cards = {DNocc, NNocc, SNocc};
            m.addConstraint(Choco.globalCardinality((IntegerVariable[]) roster[idx], shifts, cards));
            optVals[idx] = Choco.makeIntVar("opt_" + idx, Options.V_NO_DECISION);
            Choco.eq(optVals[idx], 
                    Choco.plus(Choco.abs(Choco.minus(DNocc, Choco.constant(3))), 
                    Choco.plus(Choco.abs(Choco.minus(NNocc, Choco.constant(4))), 
                    Choco.abs(Choco.minus(SNocc, Choco.constant(3))))));
//          IntegerVariable[] cards = {DNocc};
//          m.addConstraint(Choco.globalCardinality((IntegerVariable[])roster[idx], new int[]{DN}, cards));
        }
        
        IntegerExpressionVariable optVal = Choco.sum(optVals);
        m.addVariable(optVal);
        IntegerVariable[][] hours = ArrayUtils.transpose(roster);
        IntegerVariable[] totalHours = new IntegerVariable[numResources];
        int numRes = 14;
        for (int j = 0; j < numResources; j++) {
            IntegerVariable total = Choco.makeIntVar("hours_" + resources[j], 63, 84);
            //IntegerVariable total = Choco.makeIntVar("hours_" + resources[j], (j<numRes? 72: 0), (j<numRes? 100 : 1000));
            totalHours[j] = total;
            //Make an automaton that ensures that nurses work for 3 days in a row and then take at least 1 day off
            FiniteAutomaton auto = new FiniteAutomaton();
            int state = auto.addState();
            auto.setInitialState(state);
            auto.addTransition(state, state, X);
            int w1 = auto.addState();
            int w2 = auto.addState();
            int w3 = auto.addState();
            auto.addTransition(state, w1, DN);
            auto.addTransition(state, w1, SN);
            auto.addTransition(state, w1, NN);
            auto.setFinal(w3);
            auto.addTransition(w1, w2, DN);
            auto.addTransition(w1, w2, SN);
            auto.addTransition(w1, w2, NN);
            auto.addTransition(w2, w3, DN);
            auto.addTransition(w2, w3, SN);
            auto.addTransition(w2, w3, NN);
            //int w4 = auto.addState();
            auto.addTransition(w3, state, X);
            //auto.addTransition(w3, w4, X);
            //auto.addTransition(w4, state,X);
            if (j < numRes) {
                m.addConstraint(Choco.costRegular(total, (IntegerVariable[]) hours[j], auto, costs));
            }
        }
        //System.out.println(m.pretty());
        //CPSolver s = new PreProcessCPSolver();
        CPSolver s = new CPSolver();
        s.setValIntSelector(new RandomIntValSelector());
        long seed = new Date().getTime();
        BranchingFactory.randomIntSearch(s, seed);
        ChocoLogging.setVerbosity(Verbosity.FINEST);
        s.read(m);
        if (s.minimize(s.getVar(optVal), false)) {
            // Print out the solution
            System.out.print("\t");
            for (int j = 0; j < periodLength; j++) {
                System.out.print((j + 1) + "\t");
            }
            System.out.println();
            for (int j = 0; j < numResources; j++) {
                System.out.print(s.getVar(roster[0][j]).getName().split("_")[0] + ":\t");
                for (int idx = 0; idx < periodLength; idx++) {
                    System.out.print(shiftNames[s.getVar(roster[idx][j]).getVal()] + "\t");
                }
                System.out.println();
            }
            for (int j = 0; j < numResources; j++) {
                System.out.println(s.getVar(totalHours[j]).getName() + ": " + s.getVar(totalHours[j]).getVal());
            }
        } else {
            System.out.println("No solution found");
        }
    }
}
