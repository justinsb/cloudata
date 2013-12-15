//package com.cloudata.git.services;
//
//import javax.inject.Inject;
//import javax.inject.Provider;
//import javax.inject.Singleton;
//
//import com.cloudata.git.model.GitUser;
//import com.woorea.openstack.keystone.Keystone;
//import com.woorea.openstack.keystone.api.Authenticate;
//import com.woorea.openstack.keystone.model.Access;
//import com.woorea.openstack.keystone.model.Tenants;
//import com.woorea.openstack.keystone.model.authentication.TokenAuthentication;
//import com.woorea.openstack.keystone.model.authentication.UsernamePassword;
//import com.woorea.openstack.keystone.model.authentication.UsernamePassword.PasswordCredentials;
//
//@Singleton
//public class OpenstackAuthenticationManager {
//
//    @Inject
//    Provider<Keystone> keystoneProvider;
//
//    public GitUser authenticate(String username, String password) throws Exception {
//        Keystone keystone = keystoneProvider.get();
//
//        UsernamePassword authentication = new UsernamePassword();
//        PasswordCredentials passwordCredentials = new PasswordCredentials();
//        passwordCredentials.setUsername(username);
//        passwordCredentials.setPassword(password);
//        authentication.setPasswordCredentials(passwordCredentials);
//
//        // access with unscoped token
//        final Access access = keystone.execute(new Authenticate(authentication));
//
//        String token = access.getToken().getId();
//        keystone.token(token);
//
//        Tenants tenants = keystone.tenants().list().execute();
//
//        // try to exchange token using the first tenant
//        if (tenants.getList().size() > 0) {
//
//            String tenantId = tenants.getList().get(0).getId();
//
//            TokenAuthentication tokenAuthentication = new TokenAuthentication(token);
//            tokenAuthentication.setTenantId(tenantId);
//
//            Access tenantAccess = keystone.execute(new Authenticate(authentication));
//        }
//
//        // final V2AuthResponse response = identityClient.doLogin(request);
//        //
//        // if (response.access == null || response.access.token == null || response.access.user == null) {
//        // return null;
//        // }
//        //
//        // String token = response.access.token.id;
//        return new GitUser() {
//            @Override
//            public String getId() {
//                return access.getUser().getId();
//            }
//        };
//    }
//
// }
