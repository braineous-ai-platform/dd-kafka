package io.braineous.dd.producer.producer;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;


@Path("/helloworld")
public class HelloWorldResource {


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String helloWorld(
            String body
    ) {
        System.out.println("____HELLO_WORLD_____");
        System.out.println(body);
        System.out.println("___________________________");

        return body;
    }
}
