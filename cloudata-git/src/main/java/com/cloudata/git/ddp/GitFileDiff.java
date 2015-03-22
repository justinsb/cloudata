//package com.cloudata.git.ddp;
//
//import org.eclipse.jgit.diff.DiffEntry;
//import org.eclipse.jgit.diff.DiffFormatter;
//import org.eclipse.jgit.lib.ObjectId;
//
//import com.cloudata.git.model.GitRepository;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.justinsb.ddpserver.Jsonable;
//
//public class GitFileDiff implements Jsonable {
//
//  final GitRepository gitRepo;
//  final String sha;
//  final DiffEntry diffEntry;
//
//  public GitFileDiff(GitRepository gitRepo, String sha, DiffEntry diffEntry) {
//    this.gitRepo = gitRepo;
//    this.sha = sha;
//    this.diffEntry = diffEntry;
//  }
//
//  @Override
//  public JsonElement toJsonElement() {
//   
//  }
//
//}
