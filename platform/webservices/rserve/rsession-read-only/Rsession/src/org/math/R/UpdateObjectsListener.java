package org.math.R;

public interface UpdateObjectsListener {

    public void setTarget(Rsession r);

    /**Notify the changing of R environment objects*/
    public void update();
}
