package com.hftparser.writers;

import java.net.URL;

class ResolvablePath {
    private final String partialPath;
    private final String fullPath;

    public ResolvablePath(String _partialPath) {
        partialPath = _partialPath;
        fullPath = resolve(partialPath);
    }

    // answers.opencv.org/question/10236/
    private static String resolve(String partialPath) {
/*
String root = System.getProperty("user.dir");

return root + "/" + partialPath;
*/
        URL url = partialPath.getClass().getResource(partialPath);
        return url.getPath();
    }

    public String getFull() {
        return fullPath;
    }
}
