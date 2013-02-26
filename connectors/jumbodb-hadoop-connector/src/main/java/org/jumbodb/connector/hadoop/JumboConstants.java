package org.jumbodb.connector.hadoop;

/**
 * User: carsten
 * Date: 2/15/13
 * Time: 1:04 PM
 */
public class JumboConstants {

    public static final String EXPORT_ENABLED = "jumbo.export.enabled";
    public static final String IMPORT_PATH = "jumbo.import.path";
    public static final String DELIVERY_VERSION = "jumbo.delivery.version";
    public static final String DELIVERY_KEY = "jumbo.delivery.key";
    public static final String DELIVERY_INFO = "jumbo.delivery.info";
    public static final String DELIVERY_ACTIVATE = "jumbo.delivery.activate";
    public static final boolean DELIVERY_ACTIVATE_DEFAULT = true;
    public static final String HOST = "jumbo.host";
    public static final String PORT = "jumbo.port";
    public static final int PORT_DEFAULT = 12001;
    public static final String MAX_PARALLEL_IMPORTS = "jumbo.max.parallel.imports";
    public static final int MAX_PARALLEL_IMPORTS_DEFAULT = 5;
    public static final int PROTOCOL_VERSION = 3;

    public static final int BUFFER_SIZE = 32 * 1024;

    public static final String DATA_TYPE = "jumbo.data.type";
    public static final String DATA_TYPE_DATA = "jumbo.data.type.data";
    public static final String DATA_TYPE_INDEX = "jumbo.data.type.index";
}
