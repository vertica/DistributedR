package org.math.R;

import org.rosuda.REngine.REXP;
import org.junit.After;
import org.junit.Before;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import javax.swing.JFrame;
import org.junit.Test;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RserveException;

import static org.math.R.Rsession.*;

/**
 *
 * @author richet
 */
public class RsessionTest {

    PrintStream p = System.err;
    //RserverConf conf;
    Rsession s;
    int rand = Math.round((float) Math.random() * 10000);
    File tmpdir = new File("tmp"/*System.getProperty("java.io.tmpdir")*/);

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.main(RsessionTest.class.getName());
    }

    //@Test
    public void testEnd() {
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {

                public void run() {
                    Rsession s1 = Rsession.newLocalInstance(new RLogPanel(), null);
                    try {
                        System.err.println(s1.eval("runif(1)").asDouble());
                    } catch (REXPMismatchException ex) {
                        ex.printStackTrace();
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                    }
                    s1.end();
                    s1 = null;
                }
            }).start();
        }
        try {
            Thread.sleep(50000);
        } catch (InterruptedException ex) {
        }
    }

    //@Test
    public void testSIGPIPEExplicitSink() throws REXPMismatchException {
        s.SINK_OUTPUT = false;
        s.addLogger(new Logger() {

            public void println(String string, Logger.Level level) {
                System.err.println(level + " " + string);
            }

            public void close() {
            }
        });

        String loaded = s.installPackage("rgenoud", true);
        assert loaded.equals(Rsession.PACKAGELOADED) : loaded;

        // without sink: SIGPIPE error
        s.voidEval("sink(file('out.txt',open='wt'),type='output')");
        REXP maxsin = s.eval("genoud(sin, nvars=1, max=TRUE,cluster=FALSE,output.path='/tmp')");
        s.voidEval("sink(type='output')");
        //s.voidEval("unlink('out.txt')");

        assert maxsin != null : s.getLastLogEntry() + "," + s.getLastError();

        REXP test = s.eval("1+pi");
        assert test.asDouble() > 4 : "Failed next eval";
        s.SINK_OUTPUT = true;
    }

    @Test
    public void testSIGPIPEAutoSink() throws REXPMismatchException {
        s.addLogger(new Logger() {

            public void println(String string, Logger.Level level) {
                System.err.println(level + " " + string);
            }

            public void close() {
            }
        });

        String loaded = s.installPackage("rgenoud", true);
        assert loaded.equals(Rsession.PACKAGELOADED) : loaded;

        // without sink: SIGPIPE error
        REXP maxsin = s.eval("genoud(sin, nvars=1, max=TRUE,cluster=FALSE,output.path='/tmp')");
        //s.voidEval("unlink('out.txt')");

        assert maxsin != null : s.getLastLogEntry() + "," + s.getLastError();

        REXP test = s.eval("1+pi");
        assert test.asDouble() > 4 : "Failed next eval";

    }

    //@Test
    public void testFileSize() throws REXPMismatchException {
        for (int i = 0; i < 8; i++) {
            int size = (int) Math.pow(10.0, (double) i);
            s.eval("raw" + i + "<-rnorm(" + (size / 8) + ")");
            File sfile = new File("tmp", size + ".Rdata");
            s.save(sfile, "raw" + i);
            assert sfile.exists() : "Size " + size + " failed";
            p.println(sfile.length());
            sfile.delete();
        }
    }

    //@Test
    public void testJPEGSize() throws REXPMismatchException {
        s.eval("library(MASS)");
        for (int i = 1; i < 20; i++) {
            int size = i * 80;
            File sfile = new File("tmp", size + ".jpg");
            s.toJPEG(sfile, 600, 600, "parcoord(rbind(rnorm(" + (size / 8) + "),rnorm(" + (size / 8) + ")),var.label=TRUE)");
            assert sfile.exists() : "Size " + size + " failed";
            p.println(sfile.length());
        }
    }

    //@Test
    public void testPrint() throws REXPMismatchException {
        //cast
        String[] exp = {"TRUE", "0.123", "pi", /*"0.123+a",*/ "0.123", "(0.123)+pi", "rnorm(10)", "cbind(rnorm(10),rnorm(10))", "data.frame(aa=rnorm(10),bb=rnorm(10))", "'abcd'", "c('abcd','sdfds')"};
        for (String string : exp) {
            p.println(string + " --> " + s.toString(cast(s.eval(string))));
        }
    }

    //@Test
    public void testEval() throws Exception {

        double a = -0.123;
        s.set("a", a);
        s.set("b", -1.23);
        double[] A = new double[]{0, 1, 2, 3};
        s.set("A", A);

        HashMap<String, Object> vars = new HashMap<String, Object>();
        vars.put("a", 1.23);
        vars.put("A", new double[]{10, 11, 12, 13});

        String[] exp = {"TRUE", "0.123", "pi", "a", "A", "0.123+a", "0.123+b", "0.123", "(0.123)+pi", "rnorm(10)", "cbind(rnorm(10),rnorm(10))", "data.frame(aa=rnorm(10),bb=rnorm(10))", "'abcd'", "c('abcd','sdfds')"};
        for (String e : exp) {
            System.out.println(e + " --> " + s.proxyEval(e, vars));
        }
        assert Arrays.asList(s.ls()).contains("a") : "variable a disappeared";
        assert (Double) s.proxyEval("a", null) == a : "variable a changed";
        assert Arrays.equals((double[]) s.proxyEval("A", null), A) : "variable A changed";
    }

    //@Test
    public void testNullEval() throws Exception {

        double a = -0.123;
        s.set("a", a);
        assert s.eval("1+a").asDouble() == 1 + a : "error evaluating 1+a";
        s.set("a", Double.NaN);
        double res = s.eval("1+a").asDouble();
        assert Double.isNaN(res) : "error evaluating 1+a: " + res;
        s.set("a", null);
        try {
            res = s.eval("1+a").asDouble();
            throw new Exception("error evaluating 1+a: " + res);
        } catch (Exception e) {
            //Exception well raised, everything is ok.
        }

    }

    //@Test
    public void testEvalError() throws Exception {
        String[] exprs = {"a <- 1.0.0", "f <- function(x){((}"};
        for (String expr : exprs) {
            System.err.println("trying expression " + expr);
            try {
                boolean done = s.voidEval(expr);
                if (!done) {
                    throw new Exception("error not found in " + expr);
                }
            } catch (Exception e) {
                System.err.println("Well detected error in " + expr);
                //Exception well raised, everything is ok.
            }
        }

        String[] evals = {"(xsgsdfgd", "1.0.0"};
        for (String eval : evals) {
            System.err.println("trying evaluation " + eval);
            try {
                REXP e = s.eval(eval);
                if (e != null) {
                    throw new Exception("error not found in " + eval + " returned " + e.toDebugString());
                }
            } catch (Exception e) {
                System.err.println("Well detected error in " + eval);
                //Exception well raised, everything is ok.
            }
        }
    }

    //@Test
    public void testLibrary() {
        s.eval("library(lhs)");
        // this next call was failing with rserve 0.6-0
        s.eval("library(rgenoud)");
    }

    //@Test
    public void testRFile() throws REXPMismatchException {
        System.err.println("getwd(): " + s.eval("getwd()").asString());
        //System.err.println("list.files(getwd()): "+s.eval("list.files(getwd())").asString());
    }

    //@Test
    public void testRFileIO() throws REXPMismatchException {
        //get file test...
        String remoteFile1 = "get" + rand + ".csv";
        File localfile1 = new File(tmpdir, remoteFile1);
        System.out.println("GET :" + localfile1.getAbsolutePath());
        s.voidEval("aa<-data.frame(A=c(1,2,3),B=c(4,5,6))");
        s.voidEval("write.csv(file='" + remoteFile1 + "',aa)");
        InputStream is1 = null;
        OutputStream os1 = null;
        try {
            //System.out.println("openFile");
            is1 = s.connection.openFile(remoteFile1);
            //System.out.println("OK");
            os1 = new FileOutputStream(localfile1);
            byte[] buf = new byte[65536];
            try {
                s.connection.setSendBufferSize(buf.length);
            } catch (RserveException ex) {
                ex.printStackTrace();
            }
            int n = 0;
            while ((n = is1.read(buf)) > 0) {
                os1.write(buf, 0, n);
            }
            //os1.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (os1 != null) {
                    os1.close();
                }
                if (is1 != null) {
                    is1.close();
                }
            } catch (IOException ee) {
                ee.printStackTrace();
            }
        }
        assert localfile1.exists();

        //check csv file is written
        StringBuffer b = new StringBuffer();
        try {
            FileInputStream fis = new FileInputStream(localfile1);
            Reader r = new BufferedReader(new InputStreamReader(fis));
            int n = 0;
            while ((n = r.read()) > 0) {
                b.append((char) n);
            }
            r.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert b.charAt(4) == 'A' : b.charAt(4);
        s.eval("rm(aa)");
        //localfile1.delete();

        //put file test...
        String remoteFile2 = "put" + rand + ".csv";
        File localfile2 = new File(tmpdir, remoteFile2);
        System.out.println("PUT :" + localfile2.getAbsolutePath());
        String content = "A,B,C\n1,2,3\n";
        try {
            FileOutputStream fos = new FileOutputStream(localfile2);
            Writer w = new BufferedWriter(new OutputStreamWriter(fos));
            w.write(content);
            w.flush();
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //check csv file is written
        try {
            FileInputStream fis = new FileInputStream(localfile2);
            Reader r = new BufferedReader(new InputStreamReader(fis));
            int n = 0;
            while ((n = r.read()) > 0) {
                System.out.print((char) n);
            }
            r.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        InputStream is2 = null;
        OutputStream os2 = null;

        try {
            //System.out.println("createFile");
            os2 = s.connection.createFile(remoteFile2);
            //System.out.println("OK");
            is2 = new FileInputStream(localfile2);
            byte[] buf = new byte[65536];
            try {
                s.connection.setSendBufferSize(buf.length);
            } catch (RserveException ex) {
                ex.printStackTrace();
            }
            int n = 0;
            while ((n = is2.read(buf)) > 0) {
                System.out.print(buf);
                os2.write(buf, 0, n);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (os2 != null) {
                    os2.close();
                }
                if (is2 != null) {
                    is2.close();
                }
            } catch (IOException ee) {
                ee.printStackTrace();
            }
        }
        s.eval("ABC<-read.csv(file='" + remoteFile2 + "', header = TRUE,sep=',')");
        System.out.println(s.toString(s.cast(s.eval("ABC"))));
        assert s.eval("ABC$A").isNumeric();
        s.eval("rm(ABC)");
        localfile2.delete();
    }

    //@Test
    public void testCast() throws REXPMismatchException {
        //cast
        assert ((Boolean) cast(s.eval("TRUE"))) == true;
        assert ((Double) cast(s.eval("0.123"))) == 0.123;
        assert ((Double) cast(s.eval("pi"))) - 3.141593 < 0.0001;
        //assert (cast(s.eval("0.123+a"))) == null : s.eval("0.123+a").toDebugString();
        assert ((Double) cast(s.eval("0.123"))) == 0.123 : s.eval("0.123").toString();
        assert ((Double) cast(s.eval("(0.123)+pi"))) - 3.264593 < 0.0001;
        assert ((double[]) cast(s.eval("rnorm(10)"))).length == 10;
        assert ((double[][]) cast(s.eval("array(0.0,c(4,3))"))).length == 4;
        assert ((double[]) cast(s.eval("array(array(0.0,c(4,3)))"))).length == 12;
        assert ((double[][]) cast(s.eval("cbind(rnorm(10),rnorm(10))"))).length == 10;
        assert ((RList) cast(s.eval("data.frame(aa=rnorm(10),bb=rnorm(10))"))).size() == 2;
        assert ((String) cast(s.eval("'abcd'"))).equals("abcd");
        assert ((String[]) cast(s.eval("c('abcd','sdfds')"))).length == 2;
    }

    //@Test
    public void testSet() throws REXPMismatchException {

        //set
        double c = Math.random();
        s.set("c", c);
        assert ((Double) cast(s.eval("c"))) == c;

        double[] C = new double[10];
        s.set("C", C);
        assert ((double[]) cast(s.eval("C"))).length == C.length;

        double[][] CC = new double[10][2];
        CC[9][1] = Math.random();
        s.set("CC", CC);
        //System.err.println("CC[9][1]="+((double[][]) Rcast(s.evalR("CC")))[9][1]);
        assert ((double[][]) cast(s.eval("CC")))[9][1] == CC[9][1];
        assert ((double[]) cast(s.eval("CC[1,]"))).length == CC[0].length;

        System.err.println(s.cat(s.ls("C")));
        assert s.ls("C").length == 2 : "invalid ls(\"C\") : " + s.cat(s.ls("C"));

        String str = "abcd";
        s.set("s", str);
        assert ((String) cast(s.eval("s"))).equals(str);

        String[] Str = {"abcd", "cdef"};
        s.set("S", Str);
        assert ((String[]) cast(s.eval("S"))).length == Str.length;
        assert ((String) cast(s.eval("S[1]"))).equals(Str[0]);

        s.set("df", new double[][]{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}}, "x1", "x2", "x3");
        assert (Double) (cast(s.eval("df$x1[3]"))) == 7;

        //get/put files
        String[] ls = (String[]) cast(s.eval("ls()"));
        Arrays.sort(ls);
        assert ls[3].equals("c") : s.toString(ls) + "[3]=" + ls[3];
        s.eval("save(file='c" + rand + ".Rdata',c)");
        s.rm("c");
        //ls = (String[]) cast(s.eval("ls()"));
        ls = s.ls();
        Arrays.sort(ls);
        assert !ls[3].equals("c") : s.toString(ls) + "[3]=" + ls[3];
        s.eval("load(file='c" + rand + ".Rdata')");
        p.println(s.toString(cast(s.eval("c"))));

        File local = new File(tmpdir, "c" + rand + ".Rdata");
        s.receiveFile(local);
        assert local.exists();
        s.sendFile(new File(tmpdir, "c" + rand + ".Rdata"));

        //save
        File f = new File(tmpdir, "save" + rand + ".Rdata");
        if (f.exists()) {
            f.delete();
        }
        s.save(f, "C");
        assert f.exists();

        p.println("ls=\n" + s.toString(cast(s.eval("ls()"))));
        //load
        ls = (String[]) cast(s.eval("ls()"));
        Arrays.sort(ls);
        assert ls[0].equals("C") : s.toString(ls) + "[0]=" + ls[0];
        s.rm("C");
        ls = (String[]) cast(s.eval("ls()"));
        Arrays.sort(ls);
        assert !ls[0].equals("C") : s.toString(ls) + "[0]=" + ls[0];
        s.load(f);
        ls = (String[]) cast(s.eval("ls()"));
        Arrays.sort(ls);
        assert ls[0].equals("C") : s.toString(ls) + "[0]=" + ls[0];

        //toJPEG
        File jpg = new File(tmpdir, "titi" + rand + ".jpg");
        s.toJPEG(jpg, 400, 400, "plot(rnorm(10))");
        assert jpg.exists();

        //toHTML
        String html = s.asHTML("summary(rnorm(100))");
        System.out.println(html);
        assert html.length() > 0;

        //toHTML
        String txt = s.asString("summary(rnorm(100))");
        System.out.println(txt);
        assert txt.length() > 0;

        //final Rsession s2 = new Rsession(System.err);
        //p.println(toString(cast(s2.eval("0.123"))));
        //installPackage
        System.out.println(s.installPackage("sensitivity", true));
        System.out.println(s.installPackage("wavelets", true));

        //s.end();
        //s2.end();
    }

    /*//@Test
     public void testPerformance() throws REXPMismatchException { //Performance eval
     long start = Calendar.getInstance().getTimeInMillis();
     System.out.println("tic");
    
     for (int i = 0; i < 10000; i++) {
     s.silentlyEval("rnorm(10)").asDoubles();
     }
     System.out.println("toc");
     long duration = Calendar.getInstance().getTimeInMillis() - start;
     System.out.println("Spent time:" + (duration) + " ms");
     }*/
    //@Test
    public void testConcurrentEval() throws Exception {
        s.voidEval("id <- function(x){return(x)}");
        int n = 10;
        final boolean[] test = new boolean[n];
        final boolean[] done = new boolean[n];
        for (int i = 0; i < n; i++) {
            done[i] = false;
        }
        for (int i = 0; i < n; i++) {
            final int I = i;
            new Thread(new Runnable() {

                public void run() {
                    double x = Math.random();
                    try {
                        System.err.println("x= " + x);
                        double fx = -1;
                        synchronized (s) {
                            s.voidEval("x <- " + x);
                            Thread.sleep((long) (1000 + Math.random() * 1000));
                            fx = (Double) s.eval("id(x)").asDouble();
                        }
                        System.err.println(fx + " =?= " + x);
                        boolean ok = Math.abs(fx - x) < 0.00001;
                        synchronized (test) {
                            test[I] = ok;
                        }
                    } catch (Exception ex) {
                        synchronized (test) {
                            test[I] = false;
                        }
                    }
                    synchronized (done) {
                        done[I] = true;
                    }
                }
            }).start();
        }
        while (!alltrue(done)) {
            Thread.sleep(1000);
            System.err.print(".");
        }
        assert alltrue(test) : "One concurrent eval failed !";
    }

    static boolean alltrue(boolean[] a) {
        for (int i = 0; i < a.length; i++) {
            if (!a[i]) {
                return false;
            }
        }
        return true;
    }

    //@Test
    public void testConcurrency() throws InterruptedException {
        final Rsession r1 = Rsession.newInstanceTry(new Logger() {

            public void println(String string, Level level) {
                if (level == Level.INFO) {
                    System.out.println("1 " + string);
                } else {
                    System.err.println("1 " + string);
                }
            }

            public void close() {
            }
        }, null);
        final Rsession r2 = Rsession.newInstanceTry(new Logger() {

            public void println(String string, Level level) {
                if (level == Level.INFO) {
                    System.out.println("2 " + string);
                } else {
                    System.err.println("2 " + string);
                }
            }

            public void close() {
            }
        }, null);

        new Thread(new Runnable() {

            public void run() {
                try {
                    r1.eval("a<-1");

                    double a = r1.eval("a").asDouble();
                    assert a == 1 : "a should be == 1 !";
                    System.out.println("1: OK");

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }).start();
        new Thread(new Runnable() {

            public void run() {
                try {
                    r2.eval("a<-2");

                    double a2 = r2.eval("a").asDouble();
                    assert a2 == 2 : "a should be == 2 !";
                    System.out.println("2: OK");

                    double a1 = r1.eval("a").asDouble();
                    assert a1 == 1 : "a should be == 1 !";
                    System.out.println("1: OK");

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }).start();

        Thread.sleep(5000);
        r1.end();
        r2.end();
    }

    //@Test
    public void testHardConcurrency() throws REXPMismatchException, InterruptedException {
        final int[] A = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final Rsession[] R = new Rsession[A.length];
        for (int i = 0; i < R.length; i++) {
            R[i] = Rsession.newInstanceTry(new Logger() {

                public void println(String string, Level level) {
                    if (level == Level.INFO) {
                        System.out.println(string);
                    } else {
                        System.err.println(string);
                    }
                }

                public void close() {
                }
            }, null);
        }

        for (int i = 0; i < A.length; i++) {
            final int ai = A[i];
            final Rsession ri = R[i];
            //new Thread(new Runnable() {
            //
            //   public void run() {
            try {
                ri.eval("a<-" + ai);

                double ria = ri.eval("a").asDouble();
                assert ria == ai : "a should be == " + ai + " !";
                System.out.println(ai + ": OK");

            } catch (Exception ex) {
                ex.printStackTrace();
            }
            //}
            //}).start();
        }

        //checking of each Rsession to verify values are ok.
        for (int i = 0; i < A.length; i++) {
            for (int j = 0; j < i; j++) {
                while (R[j].eval("a") == null) {
                    Thread.sleep(1000);
                }
                //System.out.println("Checking " + (j + 1) + " : " + R[j]);
                double rja = R[j].eval("a").asDouble();
                assert rja == A[j] : "a should be == " + A[j] + " !";
                System.out.println(A[i] + " " + A[j] + ": OK");
            }
        }

        for (int i = 0; i < R.length; i++) {
            R[i].end();
        }
    }

    @Before
    public void setUp() {
        Logger l = new RLogPanel();
        JFrame f = new JFrame("RLogPanel");
        f.setContentPane((RLogPanel) l);
        f.setSize(600, 600);
        f.setVisible(true);
        /*Logger() {
        
         public void println(String string, Level level) {
         System.out.println(level + " " + string);
         }
        
         public void close() {
         }
         };*/
        String http_proxy_env = System.getenv("http_proxy");
        Properties prop = new Properties();
        if (http_proxy_env != null) {
            prop.setProperty("http_proxy", "\"" + http_proxy_env + "\"");
        }
        RserverConf conf = new RserverConf(null, -1/* RserverConf.RserverDefaultPort*/, null, null, prop);
        s = Rsession.newInstanceTry(l, conf);

        try {
            System.err.println(s.silentlyEval("R.version.string").asString());
        } catch (REXPMismatchException ex) {
            ex.printStackTrace();
        }
        try {
            System.err.println("Rserve version " + s.silentlyEval("installed.packages()[\"Rserve\",\"Version\"]").asString());
        } catch (REXPMismatchException ex) {
            ex.printStackTrace();
        }
        System.out.println("tmpdir=" + tmpdir.getAbsolutePath());
    }

    @After
    public void tearDown() {
        try {
            //uncomment following for sequential call. 
            //s.end();
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        s.end();
        //A shutdown hook kills all Rserve at the end.
    }
}
