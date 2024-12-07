package org.pubsubcache.api;

import jakarta.ws.rs.core.MediaType;
import org.pubsubcache.cache.CacheService;
import org.apache.pulsar.client.api.PulsarClientException;

import jakarta.servlet.ServletContext;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Path("/cache")
public class CacheResource {

    @jakarta.ws.rs.core.Context
    ServletContext context;

    @GET
    @Path("/get/{key}")
    @Produces("application/json")
    public Response get(@PathParam("key") String key) {
        CacheService cacheService = (CacheService) context.getAttribute("server");
        Object value = cacheService.get(key);
        if (value != null) {
            return Response.ok(value).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).entity("Key not found").build();
        }
    }

    @GET
    @Path("/fetch/{key}")
    @Produces("application/json")
    public Response fetch(@PathParam("key") String key) {
        try {
            CacheService cacheService = (CacheService) context.getAttribute("server");
            Object value = cacheService.fetch(key);
            return Response.ok(value).build();
        } catch (PulsarClientException | InterruptedException | ExecutionException | TimeoutException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Fetch failed: " + e.getMessage()).build();
        }
    }

    @POST
    @Path("/load")
    @Consumes("application/json")
    public Response load(@QueryParam("key") String key, @QueryParam("value") String value) {
        CacheService cacheService = (CacheService) context.getAttribute("server");
        cacheService.load(key, value);
        return Response.ok("Key loaded successfully").build();
    }

    @POST
    @Path("/put")
    @Consumes("application/json")
    public Response put(@QueryParam("key") String key, @QueryParam("value") String value) throws PulsarClientException {
        CacheService cacheService = (CacheService) context.getAttribute("server");
        cacheService.put(key, value);
        return Response.ok("Key loaded successfully").build();
    }

    @DELETE
    @Path("/invalidate/{key}")
    public Response invalidate(@PathParam("key") String key) {
        try {
            CacheService cacheService = (CacheService) context.getAttribute("server");
            cacheService.invalidate(key);
            return Response.ok("Key invalidated successfully").build();
        } catch (PulsarClientException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Invalidate failed: " + e.getMessage()).build();
        }
    }
}
