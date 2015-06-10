package org.math.R;

import java.io.File;
import java.net.Inet4Address;
import java.util.Properties;

/**
 *
 * @author richet
 */
public class Main {
        public static void main(String[] args) {
         try {
              
            //Runtime.getRuntime().exec("echo \"library(Rserve);Rserve(FALSE,args='--no-save --slave --RS-conf /tmp/Rserv.conf')\"|/Library/Frameworks/R.framework/Resources/bin/R --no-save --slave");
             
            Properties p = new Properties();
            RserverConf conf = RserverConf.parse("R://localhost");
             System.out.println(             Inet4Address.getByName("localhost").isReachable(1000));
            System.out.println(conf);
            //System.setProperty("R_HOME", "/Library/Frameworks/R.framework/Resources");
            //conf.properties = p;
            //Rsession r = new Rsession(System.out, conf, true);
          
            Rsession r = Rsession.newRemoteInstance(System.out, conf);
            System.out.println("Lancement RSession");
             System.err.println(r.silentlyEval("1+1").asString());
            
            r.installPackage("dr", true);
            r.installPackage("gam", true);
            r.installPackage("DiceKriging", true);
            r.installPackage("DimRed", new File("lib"), true);
            r.installPackage("MacsensR", new File("lib"), true);
            System.out.println("package charges");
            //r.eval("require(DimRed)");
            //r.eval("require(MacsensR)");
            
            
            r.end();
        } catch (Exception ex) {
ex.printStackTrace();
        }      
    }

}
