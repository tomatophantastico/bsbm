package benchmark.testdriver;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class UserPassPool {
  
  List<String> userpasses;
  Iterator<String> upi;
  
  public UserPassPool(File file) throws IOException {
    userpasses = Files.readLines(file, Charsets.UTF_8);
  }
  
  public String getNextUserpass(){
    if(upi == null && !upi.hasNext()){
      upi = userpasses.iterator();
      if(!upi.hasNext()){
        System.exit(0);
      }
      
    }
    
    return upi.next();
  }
  
  

}
