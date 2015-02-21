package com.cloudata.git.ddp;

import java.io.IOException;

import javax.inject.Inject;

import com.cloudata.Randoms;
import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.auth.AuthenticationManager;
import com.cloudata.auth.AuthenticationToken;
import com.cloudata.auth.PasswordCredential;
import com.cloudata.git.Escaping;
import com.google.common.base.Objects;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.justinsb.ddpserver.DdpMethod;
import com.justinsb.ddpserver.DdpMethodContext;
import com.justinsb.ddpserver.DdpMethodResult;
import com.justinsb.ddpserver.DdpPublishContext;
import com.justinsb.ddpserver.DdpSession;
import com.justinsb.ddpserver.DdpSubscription;
import com.justinsb.ddpserver.MeteorError;

public class DdpMethodLogin implements DdpMethod {

  @Inject
  AuthenticationManager authenticationManager;

  @Override
  public DdpMethodResult executeMethod(DdpMethodContext context, JsonArray params) throws Exception {
    DdpSession session = context.getSession();

    String username = null;
    String digest = null;
    String algorithm = null;
    String resume = null;

    {
      JsonObject options = params.get(0).getAsJsonObject();
      JsonObject user = options.getAsJsonObject("user");
      if (user != null) {
        username = user.get("username").getAsString();
      }
      JsonObject password = options.getAsJsonObject("password");
      if (password != null) {
        digest = password.get("digest").getAsString();
        algorithm = password.get("algorithm").getAsString();
      }
      JsonElement resumeRequest = options.get("resume");
      if (resumeRequest != null) {
        resume = resumeRequest.getAsString();
      }
    }

    if (username != null) {
      if (Objects.equal("sha-256", algorithm)) {
        AuthenticatedUser user = authenticationManager.authenticate(username, PasswordCredential.fromSha256(digest));
        if (user != null) {
          AuthenticationToken token = authenticationManager.createToken(user);

          return authenticated(session, user, token);
        } else {
          throw new MeteorError(403, "Incorrect password");
        }
      } else {
        throw new IllegalArgumentException();
      }
    } else if (resume != null) {
      ByteString tokenId = Escaping.fromBase64Url(resume);
      AuthenticationToken token = authenticationManager.findToken(tokenId);
      if (token != null) {
        AuthenticatedUser user = authenticationManager.authenticate(token);
        if (user != null) {
          return authenticated(session, user, token);
        }
      }
      throw new MeteorError(403, "You've been logged out by the server. Please log in again.");
    } else {
      throw new IllegalArgumentException();
    }
  }

  private DdpMethodResult authenticated(DdpSession session, AuthenticatedUser user, AuthenticationToken token)
      throws IOException {

    session.setState(AuthenticationToken.class, token);
    session.setState(AuthenticatedUser.class, user);

    JsonObject result = new JsonObject();
    String userId = Escaping.asBase64Url(user.getUserId());
    result.addProperty("id", userId);
    result.addProperty("token", Escaping.asBase64Url(token.getId()));
    JsonObject tokenExpires = new JsonObject();
    tokenExpires.addProperty("$date", token.getExpiration().getTime());
    result.add("tokenExpires", tokenExpires);

    {
      synchronized (session) {
        // We have to do this so that we publish the user exactly once, even if we logout and login
        DdpUserSubscription userSubscription = session.getState(DdpUserSubscription.class);
        if (userSubscription == null) {
          String subscriptionId = Escaping.asBase64Url(Randoms.buildId());
          DdpPublishContext publishContext = new DdpPublishContext(session, subscriptionId, "users");
          userSubscription = new DdpUserSubscription(publishContext, user);
          session.setState(DdpUserSubscription.class, userSubscription);
          session.addSubscription(userSubscription);
        }
      }
    }

    return DdpMethodResult.complete(result);

  }
}
