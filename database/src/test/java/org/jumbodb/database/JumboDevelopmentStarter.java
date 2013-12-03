package org.jumbodb.database;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import javax.servlet.ServletException;
import java.io.File;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:54 AM
 */
public class JumboDevelopmentStarter {
    public static void main(String[] args) throws LifecycleException, ServletException {
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(9000);
        Context context = tomcat.addWebapp("/", new File("./database/src/main/webapp/").getAbsolutePath());
        context.setEffectiveMajorVersion(3);
        tomcat.start();
        tomcat.getServer().await();
    }
}
