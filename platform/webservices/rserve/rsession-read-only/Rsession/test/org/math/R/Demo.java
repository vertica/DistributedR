package org.math.R;

import java.io.PrintStream;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.math.R.Logger.Level;
import org.rosuda.REngine.REXPMismatchException;

/**
 *
 * @author richet
 */
public class Demo {

    PrintStream p = System.err;
    Rsession R;

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.main(Demo.class.getName());
    }

    @Before
    public void setUp() {
        Logger l = new Logger() {

            public void println(String string, Level level) {
                System.out.println(level + " " + string);
            }

            public void close() {
            }
        };
        String http_proxy_env = System.getenv("http_proxy");
        Properties prop = new Properties();
        if (http_proxy_env != null) {
            prop.setProperty("http_proxy", "\"" + http_proxy_env + "\"");
        }
        RserverConf conf = new RserverConf(null, -1/* RserverConf.RserverDefaultPort*/, null, null, prop);
        R = Rsession.newInstanceTry(l, conf);

        try {
            System.err.println(R.silentlyEval("R.version.string").asString());
        } catch (REXPMismatchException ex) {
            ex.printStackTrace();
        }
        try {
            System.err.println("Rserve version " + R.silentlyEval("installed.packages()[\"Rserve\",\"Version\"]").asString());
        } catch (REXPMismatchException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        //uncomment following for sequential call. 
        R.end();
        //A shutdown hook kills all Rserve at the end.
    }

    @Test
    public void demo() throws REXPMismatchException {
        R = Rsession.newRemoteInstance(new Logger() {

            public void println(String msg, Level level) {
                System.out.println(msg);
            }

            public void close() {
            }
        }, RserverConf.parse("R://rserver.irsn.org:6311"));

        R.voidEval("require( ROpenTurns )");

        System.err.println(R.eval("packageDescription('ROpenTurns')").asString());

        R.voidEval("RS <- new( CorrelationMatrix, 4 )");
        R.voidEval("RS$set( 2, 3, -.2 )");
        
        System.err.println(R.eval("RS$show()"));
        
        R.voidEval("as( RS, 'matrix' )");

        R.voidEval("as( 1:10, NumericalPoint )");

    }
}
