package com.googlecode.connectlet.util;

import java.io.File;
import java.net.URLDecoder;

public class ClassPath extends File {
	private static final long serialVersionUID = 1L;

	private static ClassPath instance = new ClassPath();

	private static String getPath(Class<? extends ClassPath> cls) {
		try {
			String path = URLDecoder.decode(cls.getProtectionDomain().getCodeSource().getLocation().getPath(), "UTF-8");
			if (path.endsWith(".jar")) {
				path += separator + "..";
			} else if (path.endsWith(".class")) {
				int nParent = ClassPath.class.getPackage().getName().split("\\.").length + 1;
				for (int i = 0; i < nParent; i ++) {
					path += separator + "..";
				}
			}
			return new File(path).getCanonicalPath();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private ClassPath(String strFile) {
		super(strFile);
	}

	private ClassPath() {
		this(ClassPath.class);
	}

	public ClassPath(Class<? extends ClassPath> cls) {
		super(getPath(cls));
	}

	public ClassPath append(String strPath) {
		try {
			return new ClassPath(new File(getPath() + separator + strPath).getCanonicalPath());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static ClassPath getInstance() {
		return instance;
	}

	public static ClassPath getInstance(String strPath) {
		return instance.append(strPath);
	}
}