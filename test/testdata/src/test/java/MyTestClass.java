import org.msgpack.annotation.Message;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 2/26/13
 * Time: 1:23 PM
 * To change this template use File | Settings | File Templates.
 */
@Message
public class MyTestClass {
    public String str;
    public double d;
    public long l;
    public String str2;
    public List<Double> list;

    @Override
    public String toString() {
        return "MyTestClass{" +
                "str2='" + str2 + '\'' +
                ", l=" + l +
                ", d=" + d +
                ", str='" + str + '\'' +
                '}';
    }
}
