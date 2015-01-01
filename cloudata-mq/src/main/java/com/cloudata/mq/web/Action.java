package com.cloudata.mq.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class Action {
  protected HttpServletRequest request;
  protected HttpServletResponse response;

  public void init(HttpServletRequest request, HttpServletResponse response) {
    this.request = request;
    this.response = response;
  }

  public abstract void run() throws Exception;

}
