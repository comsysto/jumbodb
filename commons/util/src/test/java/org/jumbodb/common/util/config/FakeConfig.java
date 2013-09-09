package org.jumbodb.common.util.config;

/**
 * @author Ulf Gitschthaler
 */
class FakeConfig {
    private String foo;
    private String configFile;
    private String user;

    public String getFoo() {
        return foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
