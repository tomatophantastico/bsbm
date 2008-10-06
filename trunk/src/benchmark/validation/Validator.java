/*
 * Copyright (C) 2008 Andreas Schultz
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package benchmark.validation;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.util.ArrayList;

import benchmark.testdriver.TestDriverDefaultValues;

public class Validator {
	private ObjectInputStream examineStream;
	private ObjectInputStream correctStream;
	private int[] totalQueryCount;
	private int[] correctQueryCount;
	private boolean resultsCountOnly;
	
	public Validator(String testFile, String validationFile, boolean resultCountOnly) {
		try {
			examineStream = new ObjectInputStream(new FileInputStream(testFile));
			correctStream = new ObjectInputStream(new FileInputStream(validationFile));
			int maxQuery = Math.max(examineStream.readInt(), correctStream.readInt());
			totalQueryCount = new int[maxQuery];
			correctQueryCount = new int[maxQuery];
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		this.resultsCountOnly = resultCountOnly;
	}
	
	public static void main(String[] argv) {
		if(argv.length<2) {
			printUsageInfo();
			System.exit(-1);
		}
		
		Validator validator = new Validator(argv[0],argv[1],false);
		validator.test();
	}
	
	private void test() {
		try{
			System.out.println("Starting validation...\n");
			
			//Check seed
			if(examineStream.readLong()!=correctStream.readLong()) {
				System.err.println("Error: Trying to compare runs with different random number generator seeds!");
				System.exit(-1);
			}
			
			//Check scale factor
			if(examineStream.readInt()!=correctStream.readInt()) {
				System.err.println("Error: Trying to compare runs with different scale factors!");
				System.exit(-1);
			}
			
			//Check number of runs
			if(examineStream.readInt()!=correctStream.readInt()) {
				System.err.println("Error: Trying to compare runs with different query mix counts!");
				System.exit(-1);
			}
			
			Integer[] correctQuerymix = (Integer[]) correctStream.readObject();
			Integer[] examineQuerymix = (Integer[]) examineStream.readObject();
			
			//Check ignored queries
			boolean[] correctIgnoreQueries = (boolean[]) correctStream.readObject();
			boolean[] examineIgnoreQueries = (boolean[]) examineStream.readObject();
			
			for(int i=0;i<correctIgnoreQueries.length && i<examineIgnoreQueries.length;i++) {
				if(correctIgnoreQueries[i]!=examineIgnoreQueries[i]) {
					System.err.println("Error: Not the same run setup! Ignored queries (Query " + (i+1) + ") for only one run found.");
					System.exit(-1);
				}
			}
			
			for(int i=0;i<correctQuerymix.length;i++) {
				int a = correctQuerymix[i];
				int b = examineQuerymix[i];
				if(a!=b) {
					System.err.println("Error: Not the same run setup! Querymixes differ from each other at number " + (i+1) + ".");
					System.exit(-1);
				}
			}
			
			String error = null;
			QueryResult examine = null;
			QueryResult correct = null;
			//Check single query results
			while(true) {
			  try {
				examine = (QueryResult) examineStream.readObject();
				correct = (QueryResult) correctStream.readObject();
				error = null;
				
				//Check query numbers
				if(examine.getQueryNr()!=correct.getQueryNr()) {
					System.err.println("Error: Query order is different in both runs!");
					System.exit(-1);
				}
				
				totalQueryCount[examine.getQueryNr()-1]++;
				
				//Check variable names of result, only to use for comparing SPARQL results
				if(!resultsCountOnly) {
					ArrayList<String> headExamine = examine.getHeadList();
					ArrayList<String> headCorrect = examine.getHeadList();
					if(headExamine.size()!=headCorrect.size()) {
						error = addError(error, "Different count of result variables.\n");
					}
					else {
						for(int i=0;i<headExamine.size();i++) {
							if(!headExamine.get(i).equals(headCorrect.get(i))) {
								error = addError(error, "Head differs");
							}
						}
					}
				}

				//Check for Order By clause
				if(examine.isSorted()!=correct.isSorted()) {
					error = addError(error, "Trying to compare sorted results to unsorted ones.\n");
				}
				else {
					//Check results
					if(examine.getNrResults()!=correct.getNrResults()) {
						String text = "Number of results expected: " + correct.getNrResults() + "\n";
						text += "Number of results returned: " + examine.getNrResults() + "\n";
						error = addError(error, text);
					}
					else {
						//Only check content for SPARQL results 
						if(!resultsCountOnly) {
							String text = examine.compareQueryResults(correct);
	
							if(text!=null)
								error = addError(error, text);
						}
					}
				}

				if(error==null) {
					correctQueryCount[examine.getQueryNr()-1]++;
				}
				else {
					System.out.println("\nResult for Query " + examine.getQueryNr() + " of run " + examine.getRun() + " differs:\n");
					System.out.println(error);
				}
				
			  } catch(EOFException e) {
				    for(int i=0;i<totalQueryCount.length;i++) {
				    	System.out.println("Query " + (i+1) + ":");
				    	if(totalQueryCount[i]>0) {
				    		System.out.println("correct/total executions: " + correctQueryCount[i]+"/"+totalQueryCount[i]);
				    		System.out.println("correct/total ratio:" + 100*correctQueryCount[i]/totalQueryCount[i] + "%\n");
				    	}
				    	else
				    		System.out.println("Query was not executed.\n");
				    }
				    return;
			  }
			}
		} catch(IOException e) { e.printStackTrace(); System.exit(-1);}
		  catch(ClassNotFoundException e) { e.printStackTrace(); System.exit(-1); } 
	}
	
	private String addError(String errorString, String error) {
		if(errorString==null)
			errorString = error;
		else
			errorString += error;
		
		return errorString;
	}
	
	private static void printUsageInfo() {
		String output = "Usage: java benchmark.validation.Validator <options> X Y\n\n" +
		"X: file of a correct run\n\n" +
		"Y: file of a run to test\n\n" +
		"Possible options are:\n" +
		"\t-runs <number of query mix runs>\n" +
		"\t\tdefault: " + TestDriverDefaultValues.nrRuns + "\n" +
		"\t-idir <data input directory>\n" +
		"\t\tThe input directory for the Test Driver data\n" +
		"\t\tdefault: " + TestDriverDefaultValues.resourceDir + "\n" +
		"\t-qdir <query directory>\n" +
		"\t\tThe directory containing the query data\n" +
		"\t\tdefault: " + TestDriverDefaultValues.queryDir.getName() + "\n" +
		"\t-w <number of warm up runs before actual measuring>\n" +
		"\t\tdefault: " + TestDriverDefaultValues.warmups + "\n"+
		"\t-o <benchmark results output file>\n" +
		"\t\tdefault: " + TestDriverDefaultValues.xmlResultFile + "\n" +
		"\t-dg <Default Graph>\n" +
		"\t\tdefault: " + TestDriverDefaultValues.defaultGraph + "\n" +
		"\t-sql\n" +
		"\t\tuse JDBC connection to a RDBMS. Instead of a SPARQL-Endpoint, a JDBC URL has to be supplied.\n" +
		"\t\tdefault: not set\n" +
		"\t-mt <Number of clients>\n" +
		"\t\tRun multiple clients concurrently.\n" +
		"\t\tdefault: not set\n" +
		"\t-seed <Long Integer>\n" +
		"\t\tInit the Test Driver with another seed than the default.\n" +
		"\t\tdefault: " + TestDriverDefaultValues.seed + "\n" +
		"\t-t <timeout in ms>\n" +
		"\t\tTimeouts will be logged for the result report.\n" +
		"\t\tdefault: " + TestDriverDefaultValues.timeoutInMs + "ms\n" + 
		"\t-dbdriver <DB-Driver Class Name>\n" +
		"\t\tdefault: " + TestDriverDefaultValues.driverClassName+ "\n";

		System.out.print(output);
	}
}
