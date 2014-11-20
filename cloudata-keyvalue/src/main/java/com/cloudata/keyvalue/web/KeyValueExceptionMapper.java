package com.cloudata.keyvalue.web;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyValueExceptionMapper implements ExceptionMapper<Exception> {
  private static final Logger log = LoggerFactory.getLogger(KeyValueEndpoint.class);

  @Override
  public Response toResponse(Exception e) {
    log.warn("Returning error response", e);

    return Response.status(Status.BAD_REQUEST).entity(new ErrorInfo(e.getMessage())).type(MediaType.APPLICATION_JSON)
        .build();

  }

}
