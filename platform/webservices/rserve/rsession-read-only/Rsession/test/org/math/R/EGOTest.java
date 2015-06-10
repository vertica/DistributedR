package org.math.R;

import org.rosuda.REngine.REXP;
import org.math.array.DoubleArray;
import org.junit.After;
import org.junit.Before;
import java.io.File;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.Test;
import org.rosuda.REngine.REXPMismatchException;

import static org.math.R.Rsession.*;

/** Intended to reproduce the broken pipe failure.
 *
 * @author richet
 */
public class EGOTest {

    PrintStream p = System.err;
    //RserverConf conf;
    Rsession R;
    int rand = Math.round((float) Math.random() * 10000);
    File tmpdir = new File("tmp"/*System.getProperty("java.io.tmpdir")*/);

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.main(EGOTest.class.getName());
    }

    /**
    branin <- function(x) {
    x1 <- x[1]*15-5
    x2 <- x[2]*15
    (x2 - 5/(4*pi^2)*(x1^2) + 5/pi*x1 - 6)^2 + 10*(1 - 1/(8*pi))*cos(x1) + 10
    }
     */
    static double branin(double[] x) {
        double x1 = x[0] * 15 - 5;
        double x2 = x[1] * 15;
        return Math.pow(x2 - 5 / (4 * Math.PI * Math.PI) * (x1 * x1) + 5 / Math.PI * x1 - 6, 2) + 10 * (1 - 1 / (8 * Math.PI)) * Math.cos(x1) + 10;
    }
    String[] Xnames = {"x1", "x2"};
    // to emulate a noisy function

    public double[] f(double[] x) {
        return new double[]{branin(x) + Math.random() * 10.0, 7.0};
    }

    public double[][] F(double[][] X) {
        double[][] Y = new double[X.length][];
        for (int i = 0; i < Y.length; i++) {
            Y[i] = f(X[i]);
        }
        return Y;
    }

    void initR() {
        R.installPackage("DiceKriging", true);
        R.installPackage("rgenoud", true);
        R.installPackage("lhs", true);
        R.installPackage("DiceOptim", true);
        R.installPackage("DiceView", true);


        R.voidEval("max_qEI.CL.fix <- function(model, npoints, L, lower, upper, parinit=NULL, control=NULL) {"
                + "n1 <- nrow(model@X); "
                + "for (s in 1:npoints) { "
                + "oEGO <- max_EI(model, lower, upper, parinit, control); "
                + "model@X <- rbind(model@X, oEGO$par); "
                + "model@y <- rbind(model@y, L, deparse.level=0); "
                + "model@F <- trendMatrix.update(model, Xnew=data.frame(oEGO$par)); "
                + "if (model@noise.flag) { "
                + "model@noise.var = c(model@noise.var, 0)"// here is the fix!
                + " };"
                + " model <- computeAuxVariables(model); "
                + "};"
                + " return(list(par = model@X[(n1+1):(n1+npoints),, drop=FALSE], value = model@y[(n1+1):(n1+npoints),, drop=FALSE])) "
                + "}");
    }

    void initDesign() throws REXPMismatchException {
        int seed = 1;
        R.voidEval("set.seed(" + seed + ")");
        R.voidEval("Xlhs <- maximinLHS(n=9,k=2)");
        double[][] X0 = R.eval("as.matrix(Xlhs)").asDoubleMatrix();
        double[][] Xbounds = new double[][]{{0, 0}, {0, 1}, {1, 0}, {1, 1}};
        X0 = DoubleArray.insertRows(X0, 0, Xbounds);
        R.set("X" + currentiteration, X0, Xnames);
    }
    int currentiteration = -1;

    void run() throws REXPMismatchException {
        double[][] X = R.eval("as.matrix(X" + currentiteration + ")").asDoubleMatrix();
        double[][] Y = F(X);
        System.err.println(Rsession.cat(Y));
        R.set("Y" + currentiteration, DoubleArray.getColumnsCopy(Y, 0), "y");
    }

    void nextDesign() throws REXPMismatchException {
        double[][] X = R.eval("as.matrix(X" + currentiteration + ")").asDoubleMatrix();
        double[][] Y = R.eval("as.matrix(Y" + currentiteration + ")").asDoubleMatrix();

        double[][] ytomin = DoubleArray.getColumnsCopy(Y, 0);

        R.set("Y" + currentiteration, ytomin, "y");

        String nuggetnoise_str = "nugget.estim = FALSE, nugget = NULL, noise.var = ";

        double[] sdy = DoubleArray.fill(ytomin.length, 7.0);//getColumnCopy(Y, 1);
        nuggetnoise_str = nuggetnoise_str + "c(" + Rsession.cat(sdy) + ")^2";

        nuggetnoise_str = nuggetnoise_str + ", ";

        R.savels(new File("XY" + currentiteration + ".Rdata"), "" + currentiteration);

        R.voidEval("km" + currentiteration + " <- km(y~1,"
                + "optim.method='gen',"
                + "penalty = NULL,"
                + "covtype='matern5_2',"
                + nuggetnoise_str
                + "design=X" + currentiteration + ","
                + "response=Y" + currentiteration + ","
                + "control=list(" + control_km + "))");

        REXP exists = R.eval("exists('km" + currentiteration + "')");
        if (exists == null || !(exists.asInteger() == 1)) {
            R.log("No km object built:\n" + Rsession.cat(",", R.ls()), Logger.Level.ERROR);
            return;
        }

        R.savels(new File("km" + (currentiteration) + ".Rdata"), "" + (currentiteration));


        R.voidEval("EGO" + currentiteration + " <- max_qEI.CL.fix(model=km" + currentiteration + ","
                + "npoints=10,"
                //+ "L=c(" + liar + "(" + (search_min ? "" : "-") + "Y" + currentiteration + "_" + hcode + "$y)," + liar_noise + "),"
                + "L=max(Y" + currentiteration + "$y),"
                + "lower=c(0,0),"
                + "upper=c(1,1),"
                + "control=list(" + control_ego + "))");

        /*REXP*/ exists = R.eval("exists('EGO" + currentiteration + "')");
        if (exists == null || !(exists.asInteger() == 1)) {
            R.log("No EGO object built:\n" + Rsession.cat(",", R.ls()), Logger.Level.ERROR);
            return;
        }

        R.savels(new File("EGO" + (currentiteration) + ".Rdata"), "" + (currentiteration));

        R.voidEval("X" + (currentiteration + 1) + " <- rbind(X" + currentiteration + ",EGO" + currentiteration + "$par)");
    }

    void cleanRdata() {
        new File("XY" + currentiteration + ".Rdata").delete();
        new File("km" + (currentiteration) + ".Rdata").delete();
        new File("EGO" + (currentiteration) + ".Rdata").delete();
        new File("sectionview." + (currentiteration) + ".png").delete();
    }
    String control_km = "trace=FALSE,logLikFailOver=TRUE";
    String control_ego = "trace=FALSE";

    public String analyseDesign() {
        String htmlout = "";
        StringBuilder dataout = new StringBuilder();
        try {
            if (currentiteration > 0) {
                double[][] ysdy = R.eval("as.matrix(Y" + currentiteration + ")").asDoubleMatrix();;
                double[][] x = R.eval("as.matrix(X" + currentiteration + ")").asDoubleMatrix();

                double[] y = DoubleArray.getColumnCopy(ysdy, 0);

                double[] sdy = DoubleArray.fill(y.length, 7.0);//DoubleArray.getColumnCopy(ysdy, 1);

                double y0 = Double.POSITIVE_INFINITY;
                int i = -1;
                for (int ii = 0; ii < x.length; ii++) {
                    if (y[ii] < y0) {
                        i = ii;
                        y0 = y[i];
                    }
                }

                htmlout = htmlout + "Minimum value is " + y0 + " (sd=" + sdy[i] + ")<br/>";
                htmlout = htmlout + "for<br/>";
                for (int j = 0; j < x[i].length; j++) {
                    htmlout = htmlout + Xnames[j] + " = " + x[i][j] + "<br/>";
                }

                R.voidEval("pred <- predict.km(object=km" + (currentiteration - 1) + ",newdata=EGO" + (currentiteration - 1) + "$par,type='UK')");
                double em_mean = R.eval("min(pred$mean)").asDouble();
                double em_sd = R.eval("pred$sd[pred$mean==" + em_mean + "]").asDouble();

                htmlout = htmlout + "<br/>Next expected minimum value may be " + em_mean + " (sd=" + em_sd + ")";

                File f = new File("sectionview." + (currentiteration - 1) + ".png");
                R.set("bestX_" + (currentiteration - 1), x[i]);
                R.toPNG(f, 600, 600, "sectionview.km(model=km" + (currentiteration - 1) + ",center=bestX_" + (currentiteration - 1) + ",type='UK', yscale = 1,yname='Y',Xname=" + Rsession.buildListString(Xnames) + ")");
                htmlout += "\n<br/>\n<img src='" + f.getName() + "' width='600' height='600'/>";

            } else {
                htmlout = htmlout + "\n" + "Not enought results yet.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            htmlout = htmlout + "\nError: <pre>" + e.getMessage() + "</pre>";
        }

        return "<HTML name='min'>\n" + htmlout + "\n</HTML>" + dataout.toString();
    }

    /** Intended to test for an EGO algorithm of at least 500 points in 50 steps
     */
    @Test
    public void testEGO() throws REXPMismatchException {
        initR();

        currentiteration = 0;
        initDesign();

        for (currentiteration = 0; currentiteration < 50; currentiteration++) {
            run();
            System.out.println(analyseDesign());
            nextDesign();
            cleanRdata();
        }
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
        System.out.println("tmpdir=" + tmpdir.getAbsolutePath());
    }

    @After
    public void tearDown() {
        //uncomment following for sequential call. 
        //s.end();
        R.end();
        //A shutdown hook kills all Rserve at the end.
    }
}
