package com.example;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

import com.wordnik.swagger.jaxrs.config.BeanConfig;

/**
 * Main class.
 *
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://0.0.0.0:8080/myapp/";

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        String[] packages = {"com.example", "com.wordnik.swagger.jaxrs.listing"};
        final ResourceConfig rc = new ResourceConfig().packages(packages);

        rc.register(com.example.CORSResponseFilter.class);

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        BeanConfig beanConfig = new BeanConfig();
        //beanConfig.setVersion("1.0");
        beanConfig.setScan(true);
        beanConfig.setResourcePackage("com.example");
        beanConfig.setBasePath(BASE_URI);
        beanConfig.setDescription("Hello resources");
        beanConfig.setTitle("Hello API");

        final HttpServer server = startServer();
        server.getServerConfiguration().addHttpHandler(
            new StaticHttpHandler("../../www"), "/static");
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.stop();
    }
}

