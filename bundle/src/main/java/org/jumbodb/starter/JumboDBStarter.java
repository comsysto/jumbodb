package org.jumbodb.starter;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.commons.lang.StringUtils;

import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.jar.Manifest;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:54 AM
 */
public class JumboDBStarter {
    public static void main(String[] args) throws LifecycleException, ServletException {
        String applicationPath = findApplicationPath() + findFileNameFromManifest("warPath") ;
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(9000);
        tomcat.setBaseDir(findApplicationPath() + "/work/");
        tomcat.addWebapp("/",  applicationPath);
        tomcat.start();
        tomcat.getServer().await();
    }

    private static String findApplicationPath() {
        String classpath = System.getProperty("java.class.path");
        String[] split = StringUtils.split(classpath, ";:");

        String jarStarterFile = findFileNameFromManifest("jarPath");
        for (String s : split) {
            if(s.endsWith(jarStarterFile)) {
                return StringUtils.removeEnd(s, jarStarterFile);
            }
        }
        return null;
    }

    private static String findFileNameFromManifest(String type) {
        URLClassLoader cl = (URLClassLoader) JumboDBStarter.class.getClassLoader();
        try {
            URL url = cl.findResource("META-INF/MANIFEST.MF");
            Manifest manifest = new Manifest(url.openStream());
            return manifest.getMainAttributes().getValue(type);

        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
