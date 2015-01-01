package com.cloudata.mq.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class Action {

  public abstract void run(HttpServletRequest req, HttpServletResponse resp) throws Exception;

}
