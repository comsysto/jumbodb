package org.jumbodb.database;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import javax.servlet.ServletException;
import java.io.File;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:54 AM
 */
public class WebAppStart {
    public static void main(String[] args) throws LifecycleException, ServletException {
        String webappDirLocation = "./database/src/main/webapp/";
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(9000);
//        tomcat.setBaseDir("./database/");
//        tomcat.addWebapp("/", new File(webappDirLocation).getAbsolutePath());
        tomcat.addWebapp("/", "/Users/carsten/workspaces/jumbodb/database/build/libs/jumbodb-0.0.5-SNAPSHOT.war");
        tomcat.start();
        tomcat.getServer().await();
    }
}
