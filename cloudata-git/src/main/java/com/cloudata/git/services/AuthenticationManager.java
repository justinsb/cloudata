package com.cloudata.git.services;

import com.cloudata.git.model.GitUser;

public interface AuthenticationManager {

    GitUser authenticate(String username, String password) throws Exception;

}
