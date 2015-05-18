package benchmark.serializer;

import benchmark.model.Product;

public class NQuadByResource extends NTriples {

  public NQuadByResource(String file,String filesuffix, boolean forwardChaining) {
    super(file,filesuffix, forwardChaining);
  }
  
 
  
 
  
  @Override
  protected String createTriple(String subject, String predicate, String object) {
    StringBuffer result = new StringBuffer();
    result.append(subject);
    result.append(" ");
    result.append(predicate);
    result.append(" ");
    result.append(object);
    result.append(" ");
    result.append(subject);
    
    result.append(" .\n");
    
    nrTriples++;
    
    return result.toString();
  }
  
  

}
