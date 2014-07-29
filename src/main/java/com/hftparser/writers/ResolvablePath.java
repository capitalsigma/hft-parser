package com.hftparser.writers;

import java.net.URL;

public class ResolvablePath {
	String partialPath;
	String fullPath;

	public ResolvablePath(String _partialPath) {
		partialPath = _partialPath;
		fullPath = resolve(partialPath);
	}

	// answers.opencv.org/question/10236/
	public static String resolve(String partialPath) {
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
