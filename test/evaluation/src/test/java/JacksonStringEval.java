import org.codehaus.jackson.map.ObjectMapper;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 1:23 PM
 */
public class JacksonStringEval {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<String> messages = new LinkedList<String>();
        for(int i = 0; i < 1000000; i++) {
            MyTestClass cl = new MyTestClass();
            cl.d = i;
            cl.l = i;
            cl.str = "Hello" + i;
            cl.str2 = "Hello2" + i;
            cl.list = Arrays.asList(1d, 2d, 3d, 4d, 5d, 6d, 7d);
            messages.add(mapper.writeValueAsString(cl));
        }
        System.out.println("Starting deserialize");
        long start = System.currentTimeMillis();
        List<MyTestClass> res = new LinkedList<MyTestClass>();
        for (String message : messages) {
            res.add(mapper.readValue(message, MyTestClass.class));
        }
        System.out.println((System.currentTimeMillis() - start) + "ms");
    }
}
