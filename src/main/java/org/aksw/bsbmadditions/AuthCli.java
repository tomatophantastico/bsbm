package org.aksw.bsbmadditions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.nquads.NQuadsParserFactory;
import org.openrdf.rio.trig.TriGParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.Files;

public class  AuthCli  {
  
  static Logger log = LoggerFactory.getLogger(AuthCli.class);
  
  
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Throwable, RDFHandlerException, FileNotFoundException, IOException {
    Options opts = new Options();

    Option filename = OptionBuilder
        .withArgName("fileName")
        .hasArg()
        .withDescription("Location of the quad file to analyze. trig and nquads are supported.")
        .isRequired()
        .create("fn");
    opts.addOption(filename);
    Option numberOfUsers = OptionBuilder
        .withArgName("noUsers")
        .hasArg()
        .withDescription("The number of users to be generated.")
        .isRequired()
        .create("uc");
    opts.addOption(numberOfUsers);
    Option numnberOfGroups = OptionBuilder
        .withArgName("noGroups")
        .hasArg()
        .withDescription("The number of additional groups created, beside the standard ones")
        .create("gc");
    opts.addOption(numnberOfGroups);
     
    CommandLineParser clp = new BasicParser();
    CommandLine cl = null;
    try {
      cl = clp.parse(opts, args);
    
    
    String fileNameString = cl.getOptionValue("fn");
    Integer noUsersInt = Integer.parseInt( cl.getOptionValue("uc","10"));
    Integer noGroupsInt = Integer.parseInt(cl.getOptionValue("gc", "0"));
    List<String> graphs = getGraphs(fileNameString);
    AuthGenerator gen = new AuthGenerator(fileNameString + "_",noGroupsInt,noUsersInt);
    gen.generate(graphs);
    } catch (Exception e) {
      log.error( "Generation failed.  Reason: ",e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "genauth", opts );
      System.exit(0);

    }
  }
  
  protected static List<String> getGraphs(String fileNameString) throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException{
    Map<String,Integer> graphs =new  HashMap<String,Integer>();
 
    
    final AtomicInteger stmntcount = new AtomicInteger(0);
    
    RDFHandler rdfh = new RDFHandlerBase(){
      
      @Override
      public void handleStatement(Statement st) throws RDFHandlerException {
        stmntcount.incrementAndGet();
        String graph = st.getContext().stringValue();
        Integer count = graphs.get(graph);
        if(count == null){
          count = 1;
        }else{
          count++;
        }
        graphs.put(graph, count);
      }
      
    };
    RDFParser parser = null;
    if(fileNameString.endsWith(".trig")){
      parser = new TriGParserFactory().getParser();
      
    }else if(fileNameString.endsWith(".nq")){
      parser = new NQuadsParserFactory().getParser();
    }else{
      System.err.println("Files have to be either *.trig or *.nq.");
      System.exit(0);
    }
    
    parser.setRDFHandler(rdfh);
    parser.parse(new FileInputStream(fileNameString), "http://localhost/");
    
    
    TreeMultimap<Integer, String> sortedGraphs = TreeMultimap.create(); 
    
    
    
    
    
    
    for(String graph: graphs.keySet()){
      sortedGraphs.put(graphs.get(graph),graph);
    }
    
    
    BufferedWriter gtc = Files.newWriter(new File(fileNameString + "_graph_triple_count.txt"), Charset.defaultCharset());
    
    for(Integer count: sortedGraphs.keySet()){
      for(String graph: sortedGraphs.get(count)){
        gtc.write(graph + "\t" + count  + "\n");

      }
    }
    
    gtc.close();
    
    BufferedWriter tc = Files.newWriter(new File(fileNameString + "_triple_count.txt"), Charset.defaultCharset());
    tc.write("stmntscount: " + " \t " +stmntcount.get());
    tc.close();
    
    return new ArrayList<String>(graphs.keySet());
  }

}
