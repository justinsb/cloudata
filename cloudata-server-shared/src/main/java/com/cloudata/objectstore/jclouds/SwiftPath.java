//package com.cloudata.objectstore.jclouds;
//
//class SwiftPath {
//    final String container;
//    final String name;
//
//    public SwiftPath(String container, String name) {
//        this.container = container;
//        this.name = name;
//    }
//
//    static SwiftPath split(String path) {
//        while (path.startsWith("/")) {
//            path = path.substring(1);
//        }
//        int firstSlash = path.indexOf('/');
//        if (firstSlash == -1) {
//            return new SwiftPath(path, "");
//        } else {
//            return new SwiftPath(path.substring(0, firstSlash), path.substring(firstSlash + 1));
//        }
//    }
// }