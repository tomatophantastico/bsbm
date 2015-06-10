package benchmark.serializer;

import benchmark.model.Product;
import benchmark.model.Review;

/**
 * All benchmark objects, that have a Producer are put in resource specific graphs, the rest is put in a background graph.
 * 
 * @author joerg
 *
 */
public class NQuadContextProduct extends NTriples implements Serializer {

  public NQuadContextProduct(String file,String filesuffix, boolean forwardChaining) {
    super(file,filesuffix, forwardChaining);
  }
  
  @Override
  protected String convertProduct(Product product) {
    String orig =  super.convertProduct(product);
    String replaced = replaceBackgroundGraph(orig, "product/" + product.getNr());
    return replaced;
  }
  
  private String replaceBackgroundGraph(String orig, String graphsuffix){
     return orig.replaceAll("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/BackgroundGraph", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/graphs/" + graphsuffix );
  }
  
  
  @Override
  protected String createTriple(String subject, String predicate, String object) {
    StringBuffer result = new StringBuffer();
    result.append(subject);
    result.append(" ");
    result.append(predicate);
    result.append(" ");
    result.append(object);
    
    result.append(" <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/BackgroundGraph> ");
    
    result.append(" .\n");
    
    nrTriples++;
    
    return result.toString();
  }
}
