package org.math.R;

public interface Logger {

    public enum Level {
        OUTPUT,
        INFO,
        WARNING,
        ERROR;
    }

    /**Support R messages printing*/
    public void println(String message, Level l);

    public void close();
}
