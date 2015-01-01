package com.cloudata.auth;

public interface AuthenticationManager {

  AuthenticatedUser authenticate(String username, String password) throws Exception;

}
