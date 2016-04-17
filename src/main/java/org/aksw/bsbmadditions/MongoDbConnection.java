package org.aksw.bsbmadditions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
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
  private static MongoClient mclient = null;
  MongoDatabase mdb = null;
  

  
  
  public MongoDbConnection(String url, String dbname) {
    
    if(mclient==null){
      
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
  
  
  
  

  @Override
  public void executeQuery(Query query, byte queryType) {
    double timeInSeconds;
    String queryString = query.getQueryString();
    QueryMix queryMix = query.getQueryMix();
    int queryNr = query.getNr();
 
      long start = System.nanoTime();
      int resultCount = 0;

      
      String[] queryParts = query.getQueryString().split(System.getProperty("line.separator"));
      

          
      
      MongoCollection<Document> collection =  mdb.getCollection(queryParts[0]);
      
      MongoCursor<Document> iter = null;    
      //assemble the query
      if(queryParts[1].equals("aggregate")){
        List<Document> pipeline = Lists.newArrayList();
        for(int i = 2; i< queryParts.length;i++){
          String json = queryParts[i];
          if(!json.isEmpty()){
            pipeline.add( Document.parse(queryParts[i]));
          }
        }
        
       iter =  collection.aggregate(pipeline).iterator();
        
        
      }else{
        //assume find
        if (queryParts.length!=3){
          throw new RuntimeException("Query " +  query.getNr() +  "  is malformed" );
        }
        
        iter =  collection.find(Document.parse(queryParts[2])).iterator();
        
      }
      
      while (iter.hasNext()) {
        Document document = (Document) iter.next();
        resultCount++;
        
      }
          
          
          

      Long stop = System.nanoTime();
      Long interval = stop-start;
      
      timeInSeconds = interval.doubleValue()/1000000000;

      int queryMixRun = queryMix.getRun() + 1;
      
      if(logger.isEnabledFor( Level.ALL ) && queryType!=3 && queryMixRun > 0)
        logResultInfo(queryNr, queryMixRun, timeInSeconds,
                       queryString, queryType, 0,
                       resultCount);

      queryMix.setCurrent(resultCount, timeInSeconds);
  
  }

  @Override
  public void executeQuery(CompiledQuery query, CompiledQueryMix queryMix) {
    double timeInSeconds;
    String queryString = query.getQueryString();
    int queryNr = query.getNr();
 
      long start = System.nanoTime();
      int resultCount = 0;

      
      String[] queryParts = query.getQueryString().split(System.getProperty("line.separator"));
      

          
      
      MongoCollection<Document> collection =  mdb.getCollection(queryParts[0]);
      
      MongoCursor<Document> iter = null;    
      //assemble the query
      if(queryParts[1].equals("aggregate")){
        List<Document> pipeline = Lists.newArrayList();
        for(int i = 2; 2< queryParts.length;i++){
          String json = queryParts[i];
          if(!json.isEmpty()){
            pipeline.add( Document.parse(queryParts[i]));
          }
        }
        
       iter =  collection.aggregate(pipeline).iterator();
        
        
      }else{
        //assume find
        if (queryParts.length!=3){
          throw new RuntimeException("Query " +  query.getNr() +  "  is malformed" );
        }
        
        iter =  collection.find(Document.parse(queryParts[2])).iterator();
        
      }
      
      while (iter.hasNext()) {
        Document document = (Document) iter.next();
        resultCount++;
        
      }
          
          
          

      Long stop = System.nanoTime();
      Long interval = stop-start;
      
      timeInSeconds = interval.doubleValue()/1000000000;

      int queryMixRun = queryMix.getRun() + 1;
      
      if(logger.isEnabledFor( Level.ALL ) && query.getQueryType()!=3 && queryMixRun > 0)
        logResultInfo(queryNr, queryMixRun, timeInSeconds,
                       queryString, query.getQueryType(), 0,
                       resultCount);

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
      int resultCount) {
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
sb.append("\tQuery results (" + resultCount + " results): \n\n");


//sb.append(result);
sb.append("\n__________________________________________________________________________________\n");
logger.log(Level.ALL, sb.toString());
}

}
