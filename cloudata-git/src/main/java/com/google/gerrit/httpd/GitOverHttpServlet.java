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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.http.server.GitServlet;
import org.eclipse.jgit.http.server.resolver.AsIsFileService;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/** Serves Git repositories over HTTP. */
@Singleton
public class GitOverHttpServlet extends GitServlet {
  private static final long serialVersionUID = 1L;

  @Inject
  GitRepositoryStore gitRepositoryStore;

  // private static final String ATT_CONTROL = ProjectControl.class.getName();
  // private static final String ATT_RC = ReceiveCommits.class.getName();
  // private static final String ID_CACHE = "adv_bases";
  //
  // public static final String URL_REGEX;
  // static {
  // StringBuilder url = new StringBuilder();
  // url.append("^(?:/p/|/)(.*/(?:info/refs");
  // for (String name : GitSmartHttpTools.VALID_SERVICES) {
  // url.append('|').append(name);
  // }
  // url.append("))$");
  // URL_REGEX = url.toString();
  // }
  //
  // static class Module extends AbstractModule {
  // @Override
  // protected void configure() {
  // bind(Resolver.class);
  // bind(UploadFactory.class);
  // bind(UploadFilter.class);
  // bind(ReceiveFactory.class);
  // bind(ReceiveFilter.class);
  // install(new CacheModule() {
  // @Override
  // protected void configure() {
  // cache(ID_CACHE, AdvertisedObjectsCacheKey.class,
  // new TypeLiteral<Set<ObjectId>>() {
  // }).maximumWeight(4096).expireAfterWrite(10,
  // TimeUnit.MINUTES);
  // }
  // });
  // }
  // }

  public GitOverHttpServlet() {
    setRepositoryResolver(new Resolver());
    setAsIsFileService(AsIsFileService.DISABLED);

    // super.setReceivePackFactory(f);
    // getService("git-receive-pack").setEnabled(uploadsEnabled);

    // setUploadPackFactory(upload);
    // addUploadPackFilter(uploadFilter);
    //
    // setReceivePackFactory(receive);
    // addReceivePackFilter(receiveFilter);
  }

  class Resolver implements RepositoryResolver<HttpServletRequest> {

    private final boolean autoCreate = true;

    @Override
    public Repository open(HttpServletRequest req, String projectName) throws RepositoryNotFoundException,
        ServiceNotAuthorizedException, ServiceNotEnabledException {
      while (projectName.endsWith("/")) {
        projectName = projectName.substring(0, projectName.length() - 1);
      }

      if (projectName.endsWith(".git")) {
        // Be nice and drop the trailing ".git" suffix, which we never
        // keep
        // in our database, but clients might mistakenly provide anyway.
        //
        projectName = projectName.substring(0, projectName.length() - 4);
        while (projectName.endsWith("/")) {
          projectName = projectName.substring(0, projectName.length() - 1);
        }
      }

      boolean mustExist = false;

      AuthenticatedUser authenticatedUser = (AuthenticatedUser) req.getAttribute(AuthenticatedUser.class.getName());
      final GitUser user = gitRepositoryStore.toGitUser(authenticatedUser);

      GitRepository repo;
      try {
        repo = gitRepositoryStore.findRepo(projectName);
      } catch (IOException e) {
        throw new RepositoryNotFoundException(projectName, e);
      }

      if (repo == null) {
        if (autoCreate) {
          if (user == null) {
            throw new ServiceNotAuthorizedException();
          }
          try {
            repo = gitRepositoryStore.createRepo(user, projectName);
          } catch (IOException e) {
            throw new RepositoryNotFoundException(projectName, e);
          }
        } else {
          throw new RepositoryNotFoundException(projectName);
        }
      }

      if (user == null && (repo == null || !repo.isPublicRead())) {
        // We want it to prompt for a password
        throw new ServiceNotAuthorizedException();
      }

      Repository opened;
      try {
        opened = gitRepositoryStore.openRepository(user, repo, mustExist);
      } catch (IOException e) {
        RepositoryNotFoundException e2 = new RepositoryNotFoundException("Cannot open repository "
            + projectName);
        e2.initCause(e);
        throw e2;
      }
      
      if (opened == null) {
        throw new RepositoryNotFoundException(projectName);
      }
      return opened;
    }
  }

  // static class UploadFactory implements
  // UploadPackFactory<HttpServletRequest> {
  // private final TransferConfig config;
  //
  // @Inject
  // UploadFactory(TransferConfig tc) {
  // this.config = tc;
  // }
  //
  // @Override
  // public UploadPack create(HttpServletRequest req, Repository repo) {
  // UploadPack up = new UploadPack(repo);
  // up.setPackConfig(config.getPackConfig());
  // up.setTimeout(config.getTimeout());
  // return up;
  // }
  // }
  //
  // static class UploadFilter implements Filter {
  // private final Provider<ReviewDb> db;
  // private final TagCache tagCache;
  // private final ChangeCache changeCache;
  //
  // @Inject
  // UploadFilter(Provider<ReviewDb> db, TagCache tagCache,
  // ChangeCache changeCache) {
  // this.db = db;
  // this.tagCache = tagCache;
  // this.changeCache = changeCache;
  // }
  //
  // @Override
  // public void doFilter(ServletRequest request, ServletResponse response,
  // FilterChain next) throws IOException, ServletException {
  // // The Resolver above already checked READ access for us.
  // Repository repo = ServletUtils.getRepository(request);
  // ProjectControl pc = (ProjectControl) request
  // .getAttribute(ATT_CONTROL);
  // UploadPack up = (UploadPack) request
  // .getAttribute(ServletUtils.ATTRIBUTE_HANDLER);
  //
  // if (!pc.canRunUploadPack()) {
  // GitSmartHttpTools.sendError((HttpServletRequest) request,
  // (HttpServletResponse) response,
  // HttpServletResponse.SC_FORBIDDEN,
  // "upload-pack not permitted on this server");
  // return;
  // }
  //
  // if (!pc.allRefsAreVisible()) {
  // up.setAdvertiseRefsHook(new VisibleRefFilter(tagCache,
  // changeCache, repo, pc, db.get(), true));
  // }
  //
  // next.doFilter(request, response);
  // }
  //
  // @Override
  // public void init(FilterConfig config) {
  // }
  //
  // @Override
  // public void destroy() {
  // }
  // }
  //
  // static class ReceiveFactory implements
  // ReceivePackFactory<HttpServletRequest> {
  // private final AsyncReceiveCommits.Factory factory;
  // private final TransferConfig config;
  //
  // @Inject
  // ReceiveFactory(AsyncReceiveCommits.Factory factory,
  // TransferConfig config) {
  // this.factory = factory;
  // this.config = config;
  // }
  //
  // @Override
  // public ReceivePack create(HttpServletRequest req, Repository db)
  // throws ServiceNotAuthorizedException {
  // final ProjectControl pc = (ProjectControl) req
  // .getAttribute(ATT_CONTROL);
  //
  // if (!(pc.getCurrentUser() instanceof IdentifiedUser)) {
  // // Anonymous users are not permitted to push.
  // throw new ServiceNotAuthorizedException();
  // }
  //
  // final IdentifiedUser user = (IdentifiedUser) pc.getCurrentUser();
  // final ReceiveCommits rc = factory.create(pc, db)
  // .getReceiveCommits();
  // ReceivePack rp = rc.getReceivePack();
  // rp.setRefLogIdent(user.newRefLogIdent());
  // rp.setTimeout(config.getTimeout());
  // rp.setMaxObjectSizeLimit(config.getMaxObjectSizeLimit());
  // req.setAttribute(ATT_RC, rc);
  // return rp;
  // }
  // }
  //
  // static class ReceiveFilter implements Filter {
  // private final Cache<AdvertisedObjectsCacheKey, Set<ObjectId>> cache;
  //
  // @Inject
  // ReceiveFilter(
  // @Named(ID_CACHE) Cache<AdvertisedObjectsCacheKey, Set<ObjectId>> cache) {
  // this.cache = cache;
  // }
  //
  // @Override
  // public void doFilter(ServletRequest request, ServletResponse response,
  // FilterChain chain) throws IOException, ServletException {
  // boolean isGet = "GET"
  // .equalsIgnoreCase(((HttpServletRequest) request)
  // .getMethod());
  //
  // ReceiveCommits rc = (ReceiveCommits) request.getAttribute(ATT_RC);
  // ReceivePack rp = rc.getReceivePack();
  // rp.getAdvertiseRefsHook().advertiseRefs(rp);
  // ProjectControl pc = (ProjectControl) request
  // .getAttribute(ATT_CONTROL);
  // Project.NameKey projectName = pc.getProject().getNameKey();
  //
  // if (!pc.canRunReceivePack()) {
  // GitSmartHttpTools.sendError((HttpServletRequest) request,
  // (HttpServletResponse) response,
  // HttpServletResponse.SC_FORBIDDEN,
  // "receive-pack not permitted on this server");
  // return;
  // }
  //
  // final Capable s = rc.canUpload();
  // if (s != Capable.OK) {
  // GitSmartHttpTools
  // .sendError((HttpServletRequest) request,
  // (HttpServletResponse) response,
  // HttpServletResponse.SC_FORBIDDEN,
  // "\n" + s.getMessage());
  // return;
  // }
  //
  // if (!rp.isCheckReferencedObjectsAreReachable()) {
  // if (isGet) {
  // rc.advertiseHistory();
  // }
  // chain.doFilter(request, response);
  // return;
  // }
  //
  // if (!(pc.getCurrentUser() instanceof IdentifiedUser)) {
  // chain.doFilter(request, response);
  // return;
  // }
  //
  // AdvertisedObjectsCacheKey cacheKey = new AdvertisedObjectsCacheKey(
  // ((IdentifiedUser) pc.getCurrentUser()).getAccountId(),
  // projectName);
  //
  // if (isGet) {
  // rc.advertiseHistory();
  // cache.invalidate(cacheKey);
  // } else {
  // Set<ObjectId> ids = cache.getIfPresent(cacheKey);
  // if (ids != null) {
  // rp.getAdvertisedObjects().addAll(ids);
  // cache.invalidate(cacheKey);
  // }
  // }
  //
  // chain.doFilter(request, response);
  //
  // if (isGet) {
  // cache.put(cacheKey, Collections
  // .unmodifiableSet(new HashSet<ObjectId>(rp
  // .getAdvertisedObjects())));
  // }
  // }
  //
  // @Override
  // public void init(FilterConfig arg0) {
  // }
  //
  // @Override
  // public void destroy() {
  // }
  // }
}
