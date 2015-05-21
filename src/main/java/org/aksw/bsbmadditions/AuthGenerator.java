package org.aksw.bsbmadditions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.Charsets;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.nquads.NQuadsParserFactory;
import org.openrdf.rio.nquads.NQuadsWriter;
import org.openrdf.rio.nquads.NQuadsWriterFactory;
import org.openrdf.rio.trig.TriGParser;
import org.openrdf.rio.trig.TriGParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;

public class AuthGenerator {

  
  private static Logger log = LoggerFactory.getLogger(AuthGenerator.class);

  public static final String BACKGROUND_GRAPH = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/BackgroundGraph";
  
  
  
  
  public static String AUTH_FILE_PREFIX = "@prefix auth: <http://eccenca.com/elds/auth/schema/> .\n" + 
      "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" + 
      "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" + 
      "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" + 
      "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" + 
      "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n" + 
      "@prefix : <http://localhost/auth-context/> . \n" 
      //+ ":graph { \n";
      ;
 
  public static String AUTH_FILE_SUFFIX = "";// "}";
  
  
  
  public static String LDIF_PREFIX = "dn: ou=groups,dc=example,dc=com\n" + 
      "objectclass: top\n" + 
      "objectclass: organizationalUnit\n" + 
      "ou: groups\n" + 
      "\n" + 
      "dn: ou=people,dc=example,dc=com\n" + 
      "objectclass: top\n" + 
      "objectclass: organizationalUnit\n" + 
      "ou: people\n\n";



  public AuthGenerator(String fileNamePrefix, Integer noGroupsInt,
      Integer noUsersInt) {
    super();
    this.fileNamePrefix = fileNamePrefix;
    this.noGroupsInt = noGroupsInt;
    this.noUsersInt = noUsersInt;
  }
  
  private String fileNamePrefix;
  private Integer noGroupsInt;
  private Integer noUsersInt;
  
  
  
  /**
   * generate the access conditions for the graphs of the dataset.
   * @throws RDFHandlerException 
   * @throws IOException 
   */
  public void generate(List<String> graphs) throws RDFHandlerException, IOException{
    
   log.info("GRaph count is: " + graphs.size() );
    
    // make a mutable copy first
    graphs = new ArrayList<String>(graphs);
    
    
   

    
    Group admins  = new Group();
    admins.name = "admin";
    admins.graphs = new HashSet<String>(graphs);
    
    Group all = null;
    
    //deal with the default graph
    if(graphs.contains(BACKGROUND_GRAPH)){
      all = new Group();
      all.name = "all";
      all.graphs = Sets.newHashSet(BACKGROUND_GRAPH);
      graphs.remove(BACKGROUND_GRAPH);
    }
    
    List<Group> groups = Lists.newArrayList();

    //bucketing the graphs now
    Collections.shuffle(graphs);
    List<List<String>> buckets = Lists.partition(graphs, graphs.size()/10);
    
    // generate groups and associate them with the graphs.
    for(int i = 1; i<= this.noGroupsInt;i++){
      Group g = new Group();
      g.graphs = new HashSet<String>();
      g.name = "group_" + i;
      groups.add(g);
      
      //associate 0-9 buckets to the group
      int bucketCount = new Random().nextInt(9);
      for(int j = 0;j<bucketCount;j++){
        int randombBucket = new Random().nextInt(9);
        g.graphs.addAll(buckets.get(randombBucket));
      }
      
    }
    
   
    
    
    //generating admin users
    // 5% of our users are admins
    int adminUsercount =  ((Double) Math.ceil(this.noUsersInt * 0.05 )).intValue();
    int otherUserCount = this.noUsersInt - adminUsercount;
    
    List<User> users = Lists.newArrayList();
    
    //generate admins
    for(int i = 0; i<adminUsercount; i++){
      User adminuser = new User();
      adminuser.name = "admin_" + i; 
      adminuser.groups.add(admins);
      
      admins.users.add(adminuser);
      users.add(adminuser);
    }
    
    //generate other users
    
    for(int i = 0; i<otherUserCount; i++){
      User otheruser = new User();
      otheruser.name = "user_" + i; 
      if(all!=null){
        otheruser.groups.add(all);
        all.users.add(otheruser);
      }
      
      //add 2-6 groups to a user
      int groupCount = new Random().nextInt(4) + 2;
      for(int j = 0;j<groupCount;j++){
        int randomGroupInt = new Random().nextInt(groups.size());
        Group randomGroup = groups.get(randomGroupInt);
        otheruser.groups.add(randomGroup);
        randomGroup.users.add(otheruser);
        
      }
      users.add(otheruser);
    }
    
    log.info("Generated users and groups");
    
    //open the files
    BufferedWriter auth_ntrig =  Files.newWriter(new File("./auth.ttl"), Charsets.UTF_8);
    BufferedWriter ldif =  Files.newWriter(new File("./auth.ldif"), Charsets.UTF_8);
    BufferedWriter userList =  Files.newWriter(new File("./users.list"), com.google.common.base.Charsets.UTF_8);
    BufferedWriter userGraphCount = Files.newWriter(new File("./users_graph_count.list"), com.google.common.base.Charsets.UTF_8);
    BufferedWriter groupGraphCount = Files.newWriter(new File("./groups_graph_count.list"), com.google.common.base.Charsets.UTF_8);
    BufferedWriter auth_session = Files.newWriter(new File("./auth_session.ttl"), com.google.common.base.Charsets.UTF_8);
    BufferedWriter auth_session_materialized = Files.newWriter(new File("./auth_session_mat.ttl"), com.google.common.base.Charsets.UTF_8);

    auth_ntrig.write(AUTH_FILE_PREFIX);
    auth_session.write(AUTH_FILE_PREFIX);
    auth_session_materialized.write(AUTH_FILE_PREFIX);
    ldif.write(LDIF_PREFIX);
    //write them to disk;
    writeAccessCondition(admins, auth_ntrig);
    writeGroupLdif(admins, ldif);
    
    for(Group group : groups){
      writeAccessCondition(group, auth_ntrig);
      writeGroupLdif(group, ldif);
    }
    
    
    writeUserList(users,userList);
    writeUsersLdif(users, ldif);
    writerUserGRaphCount(users,userGraphCount);
    writerGRoupGRaphCount(groups, groupGraphCount);
    
    writeSessionData(users,auth_session);
    writeSessionMat(users,auth_session_materialized);
    
    // close
    auth_ntrig.write(AUTH_FILE_SUFFIX);
    auth_ntrig.close();
    ldif.close();
    userList.close();
    userGraphCount.close();
    groupGraphCount.close();
    auth_session.close();
    auth_session_materialized.close();
    log.info("finished writing groups and users");
 
  }


  
  
  
  private void writeSessionMat(List<User> users,
      BufferedWriter out) throws IOException {
    for(User user: users){
      for(Group group: user.groups){
        for(String graph : group.graphs){
          out.write(String.format(":%1$s auth:readGraph <%2$s>.\n", user.name, graph));
        }
      }
    }
  }





  private void writeSessionData(List<User> users, BufferedWriter out) throws IOException {
    for(User user: users){
    
      out.write(String.format(":%1$s a auth:Account ;\n" + 
          "    auth:login \"%1$s\";\n"  , user.name));
      if(user.groups!=null&& user.groups.size()>0){
        List<String> groupnames = Lists.transform(user.groups, Functions.toStringFunction());
        out.write(String.format("    auth:memberOf :%s ;\n", Joiner.on(", :").join(groupnames)));
      }
      out.write(String.format(".\n :Session%1$s a auth:Session ;\n" + 
          "    auth:openedBy :%1$s . \n\n",user.name));
      
     
    }
    
  }





  private void writerUserGRaphCount(List<User> users,
      BufferedWriter userGraphCount) throws IOException {
    for(User user: users){
      Set<String> graphs= Sets.newHashSet();
      for(Group group: user.groups){
        graphs.addAll(group.graphs);
      }
      userGraphCount.write(String.format("%s \t %d \n", user.name, graphs.size()));
    }
    
  }
  
  private void writerGRoupGRaphCount(List<Group> groups,
      BufferedWriter out) throws IOException {
    for(Group  group: groups){
      out.write(String.format("%s \t %d \n", group.name, group.graphs.size()));
    }
    
  }





  public static class User{
    String name;
    List<Group> groups = Lists.newArrayList();
  }
  
  
  public static class Group{
    String name;
    Set<String> graphs = new HashSet<String>();
    List<User> users = Lists.newArrayList();
    
    @Override
    public String toString() {
      return name;
    }
    
    
  }

  private void writeAccessCondition(Group group, BufferedWriter out) throws IOException{
    out.write(String.format("   :%1$sAccessCondition a auth:AccessCondition ;\n" + 
        "    auth:requiresGroup :%1$s ;\n", group.name)); 
    if(!(group.graphs == null || group.graphs.isEmpty() )){
      
      //no joiners here, as too memory costly
      Iterator<String> gi = group.graphs.iterator();
      out.write(String.format("auth:readGraph   <%s>",gi.next()));
      
      while(gi.hasNext()){
        out.write(String.format(", <%s>",gi.next()));
      }
        
    }
    out.write(". \n");   
  }
  
  
  private void writeGroupLdif(Group group, BufferedWriter out) throws IOException{
   out.write(String.format("dn: cn=%1$s,ou=groups,dc=example,dc=com\n" + 
       "objectclass: top\n" + 
       "objectclass: groupOfNames\n" + 
       "cn: %1$s\n",group.name));
   for(User user: group.users){
     out.write(String.format("uniqueMember: uid=%1$s,ou=people,dc=example,dc=com\n",user.name));
   }
   out.write("\n");
       
   
    
  }
  

  
  
  private void writeUsersLdif(List<User> users, BufferedWriter out) throws IOException{
 for(User user: users){
      
      out.write(String.format("dn: uid=%1$s,ou=people,dc=example,dc=com\n" + 
          "objectclass: top\n" + 
          "objectclass: person\n" + 
          "objectclass: organizationalPerson\n" + 
          "objectclass: inetOrgPerson\n" + 
          "cn: %1$s\n" + 
          "sn: %1$s\n" + 
          "uid: %1$s\n" + 
          "userPassword: %1$s\n\n",user.name));
    }
    
    
    
  }
  
  
  
  private void writeUserList(List<User> users, BufferedWriter out) throws IOException{
    
    for(User user: users){
      out.write(String.format("%1$s:%1$s\n",user.name));
    }
    
  }
  
  
}
