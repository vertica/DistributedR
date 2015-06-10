package com.example;


import java.net.URI;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import javax.inject.Singleton;

import com.wordnik.swagger.annotations.*;

import com.example.Cluster;
//import com.myvertica.websocketexample.RSessionManager;
//import com.myvertica.websocketexample.SqlSessionManager;
import com.example.BackendConstants;

/**
 * Root resource (exposed at "myresource" path)
 */
@Singleton
@Path("clusters")
@Api(value = "/clusters", description = "API to manager clusters.")
public class ClusterResource {

	private final Map<Integer, Cluster> clusterDB =
			new ConcurrentHashMap<Integer, Cluster>();
	private AtomicInteger idCounter = new AtomicInteger();

	private int index = 0;
    
	
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Say Hello World",
            notes = "Anything Else?")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 500, message = "Something wrong in Server")})
    public ArrayList<Cluster> getCustomer() {
    	return new ArrayList<Cluster>(clusterDB.values());
    }
    
    //create new cluster    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createCustomer(Cluster cluster) {
	    
	    cluster.setId(idCounter.incrementAndGet());
	    
	    clusterDB.put(cluster.getId(), cluster);
	    System.out.println("Created cluster " + cluster.getId() + " name: "+cluster.getName() + " memory: " + cluster.getMemory());
            //Map<String,String> mm2 = RSessionManager.getInstance().runCommand("distributedR_start(cluster_conf=\""+BackendConstants.CLUSTER_CONF_XML_PATH+"\", workers=FALSE, yarn=TRUE)");
	    
	    return Response.created(URI.create("/clusters/"
    			+ cluster.getId())).build();
    }
    
    @POST
    @Path("{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Cluster updateCustomerViaPost(@PathParam("id") int id, Cluster update) {
	    
    	return doUpdate(id, update);
        
    }
    
    //update a cluster    
    @PUT
    @Path("{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Cluster updateCustomerViaPut(@PathParam("id") int id, Cluster update) {
	    
    	return doUpdate(id, update);
        
    }
    private Cluster doUpdate(int id, Cluster update) {
    	Cluster current = clusterDB.get(id);
        if (current == null)
          throw new WebApplicationException(Response.Status.NOT_FOUND);

        current.setName(update.getName());
        current.setMemory(update.getMemory());
        current.setStatus(update.getStatus());
        System.out.println("Updated cluster "+id);
        return current;
    	
    }
    
    @DELETE
    @Path("{id}")    
    public Response deleteCustomer(@PathParam("id") int id) {
	    System.out.println("removed cluster "+id);
    	clusterDB.remove(id);        
        //Map<String,String> mm = RSessionManager.getInstance().runCommand("distributedR_shutdown()");
        return Response.status(200).build();        
    }
    
    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Cluster getCluster(@PathParam("id") int id) {
            final Cluster cluster = clusterDB.get(id);
            if (cluster == null) {
                    throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
            return cluster;
    }
    
//    @GET
//    @Path("{id}")
//    @Produces(MediaType.APPLICATION_JSON)
//	public StreamingOutput getCustomer(@PathParam("id") int id) {
//		final Customer customer = customerDB.get(id);
//		if (customer == null) {
//			throw new WebApplicationException(Response.Status.NOT_FOUND);
//		}
//		return new StreamingOutput() {
//			public void write(OutputStream outputStream) throws IOException,
//					WebApplicationException {
//				outputCustomer(outputStream, customer);
//			}
//		};
//    }



}
