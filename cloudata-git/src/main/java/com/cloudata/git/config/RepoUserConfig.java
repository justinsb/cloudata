//package com.cloudata.git.config;
//
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.List;
//
//import com.google.common.io.CharStreams;
//
//public class RepoUserConfig {
//
//  private String name;
//
//  private RepoUserConfig(String name, List<String> lines) {
//    this.name = name;
//  }
//
//  public static RepoUserConfig parse(String name, InputStreamReader reader) throws IOException {
//    List<String> lines = CharStreams.readLines(reader);
//    return new RepoUserConfig(name, lines);
//  }
//
//}
