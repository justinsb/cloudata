//// Copyright (C) 2012 The Android Open Source Project
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
//package com.cloudata.auth;
//
//import java.io.IOException;
//
//import javax.servlet.Filter;
//import javax.servlet.FilterChain;
//import javax.servlet.FilterConfig;
//import javax.servlet.ServletException;
//import javax.servlet.ServletRequest;
//import javax.servlet.ServletResponse;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.inject.Singleton;
//
///**
// * Formats uncaught exceptions nicely.
// * 
// * Not sure why Jetty isn't doing this for us...
// *
// */
//@Singleton
//public class ServletErrorFilter implements Filter {
//  private static final Logger log = LoggerFactory.getLogger(ServletErrorFilter.class);
//
//  @Override
//  public void init(FilterConfig config) {
//  }
//
//  @Override
//  public void destroy() {
//  }
//
//  @Override
//  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
//      ServletException {
//    HttpServletRequest req = (HttpServletRequest) request;
//    HttpServletResponse rsp = (HttpServletResponse) response;
//
//    try {
//      chain.doFilter(req, rsp);
//    } catch (Exception e) {
//      log.error("Unexpected error processing http request", e);
//      rsp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//    }
//  }
//}
