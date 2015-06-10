import java.io.File;
import java.util.Properties;
import org.math.R.*;
import org.rosuda.REngine.*;

public class Example1 { 
    public static void main(String args[]) {
        Properties p = new Properties();
        RserverConf conf = new RserverConf("localhost", 6311, "", "", p);
        Rsession s = Rsession.newInstanceTry(System.out, conf);
        double[] rand = null;
        try {
            rand = (double[]) Rsession.cast(s.eval("rnorm(10)")); //create java variable from R command
        }
        catch(REXPMismatchException e) {
            System.out.println(e);
        }
 
 
        s.set("c", Math.random()); //create R variable from java one
 
        s.save(new File("save.Rdata"), "c"); //save variables in .Rdata
        s.rm("c"); //delete variable in R environment
        s.load(new File("save.Rdata")); //load R variable from .Rdata
 
 
        s.set("df", new double[][]{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}}, "x1", "x2", "x3"); //create data frame from given vectors
        double value = 0.0;
        try {
           value = (Double) (Rsession.cast(s.eval("df$x1[3]"))); //access one value in data frame
        }
        catch(REXPMismatchException e) {
             System.out.println(e);
        }
 
        s.toJPEG(new File("plot.jpg"), 400, 400, "plot(rnorm(10))"); //create jpeg file from R graphical command (like plot)
 
        String html = s.asHTML("summary(rnorm(100))"); //format in html using R2HTML
        System.out.println(html);
 
        String txt = s.asString("summary(rnorm(100))"); //format in text
        System.out.println(txt);
 
 
        System.out.println(s.installPackage("sensitivity", true)); //install and load R package
        System.out.println(s.installPackage("wavelets", true));
 
        s.end();
    }
}
