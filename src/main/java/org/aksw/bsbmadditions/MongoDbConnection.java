package org.aksw.bsbmadditions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import benchmark.qualification.QueryResult;
import benchmark.testdriver.CompiledQuery;
import benchmark.testdriver.CompiledQueryMix;
import benchmark.testdriver.Query;
import benchmark.testdriver.QueryMix;
import benchmark.testdriver.ServerConnection;

public class MongoDbConnection implements ServerConnection{
  
  private static Logger logger = Logger.getLogger( MongoDbConnection.class );

  
  // the connection pool is using a singleton style implementation
  private MongoClient mclient = null;
  private MongoDatabase mdb = null;
  

  
  
  public MongoDbConnection(String url, String dbname) {
      
   
    if(mdb ==null){
      synchronized (MongoDbConnection.class) {
        

      LogManager.getLogger("org.mongodb.driver.connection").setLevel(org.apache.log4j.Level.OFF);
      LogManager.getLogger("org.mongodb.driver.management").setLevel(org.apache.log4j.Level.OFF);
      LogManager.getLogger("org.mongodb.driver.cluster").setLevel(org.apache.log4j.Level.OFF);
      LogManager.getLogger("org.mongodb.driver.protocol.insert").setLevel(org.apache.log4j.Level.OFF);
      LogManager.getLogger("org.mongodb.driver.protocol.query").setLevel(org.apache.log4j.Level.OFF);
      LogManager.getLogger("org.mongodb.driver.protocol.update").setLevel(org.apache.log4j.Level.OFF);
      
      mclient = new MongoClient(url);
      mdb = mclient.getDatabase(dbname);
    }
    }
  }
  
  
  
  

  @Override
  public void executeQuery(Query query, byte queryType) {
    double timeInSeconds;
    String queryString = query.getQueryString();
    QueryMix queryMix = query.getQueryMix();
    int queryNr = query.getNr();
 
      long start = System.nanoTime();
      int resultCount = 0;

      
      String[] queryParts = query.getQueryString().split(System.getProperty("line.separator"));
      

      LinkedHashMultimap<String, String> fetch = LinkedHashMultimap.create();
      String fetchExec = null;
      
      for(String queryPart: queryParts){
        if(queryPart.startsWith("{")){
          fetch.put(fetchExec, queryPart);
        }else{
          fetchExec = queryPart;
        }
        
      }
      //This will not work in the general case, but only for the queries we have.
      Multimap<String,Document> results = HashMultimap.create();
      for(String exec: fetch.keySet()){
        String[] execSplit  = exec.split("\\.");
        String collection  = execSplit[0];
        String operation = execSplit[1];
        if(operation.equals("aggregate")){
         results.putAll( executeAggregate(collection,fetch.get(exec),results));
        }else{
          results.putAll( executeFind(collection,fetch.get(exec).iterator().next(),results));
        }
      }
      
      
     
      resultCount = results.values().size();
          
          

      Long stop = System.nanoTime();
      Long interval = stop-start;
      
      timeInSeconds = interval.doubleValue()/1000000000;

      int queryMixRun = queryMix.getRun() + 1;
      
      if(logger.isEnabledFor( Level.ALL ) && queryType!=3 && queryMixRun > 0)
        logResultInfo(queryNr, queryMixRun, timeInSeconds,
                       queryString, queryType, 0,
                       resultCount,results);

      queryMix.setCurrent(resultCount, timeInSeconds);
  
  }

  private Multimap<String,Document> executeAggregate(String collection, Set<String> pipelineStrings, Multimap<String, Document> previousresults) {
    
    List<Document> pipeline = Lists.newArrayList();
    for(String pipelineString:pipelineStrings){
        pipeline.add( Document.parse(pipelineString));
    }
    
    MongoCollection<Document> mdbColl =  mdb.getCollection(collection);
    MongoCursor<Document> aggCursor =  mdbColl.aggregate(pipeline).iterator();
    
    Multimap<String, Document> result = HashMultimap.create();
    while(aggCursor.hasNext()){
      result.put(collection, aggCursor.next());
    }
    return result;

  }
  
  
  private Multimap<String,Document> executeFind(String collection, String find,Multimap<String,Document> previousResults){
    Multimap<String,Document> result = HashMultimap.create();
    MongoCollection<Document> mdbColl =  mdb.getCollection(collection);
    for(String findTemplated: applyTemplate(find, previousResults)){
      MongoCursor<Document> aggCursor =  mdbColl.find(Document.parse(findTemplated)).iterator();
      while(aggCursor.hasNext()){
        result.put(collection, aggCursor.next());
      }

    }
    
    return result;
    
    
  }

  
  private List<String> applyTemplate(String origQuery, Multimap<String, Document> previousResults){
    List<String> templateFilled = Lists.newArrayList();
    String pattern = origQuery;
    if(origQuery.contains("#")){
      int firstHashLoc = origQuery.indexOf("#");
      int secondHashLoc = origQuery.indexOf("#", firstHashLoc+1);
      pattern = origQuery.substring(firstHashLoc+1,secondHashLoc);
    
   
      String collection = pattern.substring(0, pattern.indexOf("."));
      String key = pattern.substring(pattern.indexOf(".")+1);
      
      for(Document doc: previousResults.get(collection)){
        for(String patternFillString : getValues(doc, key)){
          templateFilled.add(origQuery.replace("#"+pattern+"#", patternFillString));
        }
      }
     
    
    }else{
      templateFilled.add( origQuery);
    }
    
    return templateFilled;
    
  }
  

  
  
  private List<String> getValues(Document doc, String key){
    List<String> results = Lists.newArrayList();
    if(key.contains(".")){
      String topKey = key.substring(0, key.indexOf("."));
      String subkey = key.substring(key.indexOf(".")+1);
      results.addAll(getValues((Document)doc.get(topKey), subkey));
    }else{
      results.add(doc.get(key).toString());
    }
    
    return results;
    
    
  }


  @Override
  public void executeQuery(CompiledQuery query, CompiledQueryMix queryMix) {
    double timeInSeconds;
    String queryString = query.getQueryString();
    int queryNr = query.getNr();
 
      long start = System.nanoTime();
      int resultCount = 0;

      
      String[] queryParts = query.getQueryString().split(System.getProperty("line.separator"));
      

      LinkedHashMultimap<String, String> fetch = LinkedHashMultimap.create();
      String fetchExec = null;
      
      for(String queryPart: queryParts){
        if(queryPart.startsWith("{")){
          fetch.put(fetchExec, queryPart);
        }else{
          fetchExec = queryPart;
        }
        
      }
      //This will not work in the general case, but only for the queries we have.
      Multimap<String,Document> results = HashMultimap.create();
      for(String exec: fetch.keySet()){
        String[] execSplit  = exec.split("\\.");
        String collection  = execSplit[0];
        String operation = execSplit[1];
        if(operation.equals("aggregate")){
         results.putAll( executeAggregate(collection,fetch.get(exec),results));
        }else{
          results.putAll( executeFind(collection,fetch.get(exec).iterator().next(),results));
        }
      }
      
      
     
      resultCount = results.values().size();
          
          

      Long stop = System.nanoTime();
      Long interval = stop-start;
      
      timeInSeconds = interval.doubleValue()/1000000000;

      int queryMixRun = queryMix.getRun() + 1;
      
      if(logger.isEnabledFor( Level.ALL ) && query.getQueryType()!=3 && queryMixRun > 0)
        logResultInfo(queryNr, queryMixRun, timeInSeconds,
                       queryString, query.getQueryType(), 0,
                       resultCount,results);

      queryMix.setCurrent(resultCount, timeInSeconds);

  }

  
  
  
  @Override
  public QueryResult executeValidation(Query query, byte queryType) {
    return null;
  }

  @Override
  public void close() {

  }
  
  private void logResultInfo(int queryNr, int queryMixRun, double timeInSeconds,
      String queryString, byte queryType, int resultSizeInBytes,
      int resultCount, Multimap<String,Document> results) {
  StringBuffer sb = new StringBuffer(1000);
  sb.append("\n\n\tQuery " + queryNr + " of run " + queryMixRun + " has been executed ");
  sb.append("in " + String.format("%.6f",timeInSeconds) + " seconds.\n" );
  sb.append("\n\tQuery string:\n\n");
  sb.append(queryString);
  sb.append("\n\n");
  
  //Log results
  if(queryType==Query.DESCRIBE_TYPE)
  sb.append("\tQuery(Describe) result (" + resultSizeInBytes + " Bytes): \n\n");
  else
  sb.append("\tQuery results:" + resultCount + " (count)");
  for(Document doc: results.values()){
    sb.append(doc.toJson());
    sb.append("\n");
  }
  sb.append("\n\n");
  
  
  //sb.append(result);
  sb.append("\n__________________________________________________________________________________\n");
  logger.log(Level.ALL, sb.toString());
  }

}
