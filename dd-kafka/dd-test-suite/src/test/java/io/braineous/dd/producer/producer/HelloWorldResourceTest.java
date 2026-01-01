package io.braineous.dd.producer.producer;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class HelloWorldResourceTest {

    @Test
    public void test_drive(){
        RestAssured.given()
                .contentType(MediaType.APPLICATION_JSON)
                .body("{}")
                .post("/helloworld")
                .then()
                .statusCode(200);
    }
}
