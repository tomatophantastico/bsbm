package org.aksw.bsbmadditions;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

public class AUTH {
  
  public static String NAMESPACE = "http://eccenca.com/auth";
  
  public static String PREFIX = "auth";
  
  public static URI ACCOUNT;
  public static URI LOGIN;
  public static URI PASSWORD_SHA256SUM;
  public static URI MEMBEROF;
  public static URI ACCESSCONDITION;
  public static URI READGRAPH;
  public static URI WRITEGRAPH;
  public static URI REQUIRESGROUP;
  
  
  
  static {
    ValueFactory factory = ValueFactoryImpl.getInstance();
    ACCOUNT = factory.createURI(NAMESPACE, "Account");
    LOGIN  = factory.createURI(NAMESPACE, "login");
    PASSWORD_SHA256SUM  = factory.createURI(NAMESPACE, "password_sha256sum");
    MEMBEROF = factory.createURI(NAMESPACE, "memberof");
    ACCESSCONDITION = factory.createURI(NAMESPACE, "AccessCondition");
    READGRAPH = factory.createURI(NAMESPACE, "readGraph");
    WRITEGRAPH = factory.createURI(NAMESPACE, "writeGraph");
    REQUIRESGROUP = factory.createURI(NAMESPACE, "requiresGroup");

        
  }
}
