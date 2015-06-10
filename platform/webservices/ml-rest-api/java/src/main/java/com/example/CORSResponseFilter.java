package com.example;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import com.example.BackendConstants;
 
public class CORSResponseFilter implements ContainerResponseFilter {
 
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
        throws IOException {
 
            responseContext.getHeaders().add("Access-Control-Allow-Origin", BackendConstants.ACCESS_CONTROL_ALLOW_ORIGIN);
            //responseContext.getHeaders().add("Access-Control-Allow-Origin", "http://133-1.bfc.hpl.hp.com:5002,http://15.9.72.172:5002,http://localhost:5002");
            responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true");
            responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE, PUT");
            responseContext.getHeaders().add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    }
} 
