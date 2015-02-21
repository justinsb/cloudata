package com.cloudata.git.ddp;

import javax.inject.Inject;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.auth.AuthenticationManager;
import com.cloudata.auth.AuthenticationToken;
import com.google.gson.JsonArray;
import com.justinsb.ddpserver.DdpMethod;
import com.justinsb.ddpserver.DdpMethodContext;
import com.justinsb.ddpserver.DdpMethodResult;
import com.justinsb.ddpserver.DdpSession;

public class DdpMethodLogout implements DdpMethod {

  @Inject
  AuthenticationManager authenticationManager;

  @Override
  public DdpMethodResult executeMethod(DdpMethodContext context, JsonArray params) throws Exception {
    DdpSession session = context.getSession();

    AuthenticationToken authenticationToken = session.getState(AuthenticationToken.class);
    AuthenticatedUser user = session.getState(AuthenticatedUser.class);

    if (authenticationToken != null) {
      authenticationManager.revokeToken(authenticationToken);
    }

    session.setState(AuthenticationToken.class, null);
    session.setState(AuthenticatedUser.class, null);

    return DdpMethodResult.complete(null);
  }
}
