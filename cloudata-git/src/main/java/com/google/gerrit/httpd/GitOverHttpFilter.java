// Copyright (C) 2010 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.gerrit.httpd;

import org.eclipse.jgit.http.server.GitFilter;
import org.eclipse.jgit.http.server.resolver.AsIsFileService;

import com.cloudata.git.services.GitRepositoryStore;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/** Serves Git repositories over HTTP. */
@Singleton
public class GitOverHttpFilter extends GitFilter {
  private static final long serialVersionUID = 1L;

  @Inject
  public GitOverHttpFilter(GitRepositoryStore gitRepositoryStore) {
    setRepositoryResolver(new GitRepositoryResolver(gitRepositoryStore));
    setAsIsFileService(AsIsFileService.DISABLED);
  }
}