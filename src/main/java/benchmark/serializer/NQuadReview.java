package benchmark.serializer;

import benchmark.model.Product;
import benchmark.model.Review;

/**
 * All benchmark objects, that have a Producer are put in resource specific graphs, the rest is put in a background graph.
 * 
 * @author joerg
 *
 */
public class NQuadReview extends NTriples implements Serializer {

  public NQuadReview(String file,String filesuffix, boolean forwardChaining) {
    super(file,filesuffix, forwardChaining);
  }
  
  @Override
  protected String convertReview(Review review) {
    String orig =  super.convertReview(review);
    String replaced = replaceBackgroundGraph(orig, "/review/" + review.getNr());
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
