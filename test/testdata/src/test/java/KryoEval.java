import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.OutputChunked;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 1:23 PM
 */
public class KryoEval {

    public static void main(String[] args) throws Exception {
        Kryo kryo = new Kryo();
        kryo.register(MyTestClass.class);

        List<byte[]> messages = new LinkedList<byte[]>();
        for(int i = 0; i < 5000000; i++) {
            MyTestClass cl = new MyTestClass();
            cl.d = i;
            cl.l = i;
            cl.str = "Hello" + i;
            cl.str2 = "Hello2" + i;
            cl.list = Arrays.asList(1d, 2d, 3d,4d, 5d, 6d, 7d);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            kryo.writeObject(new OutputChunked(byteArrayOutputStream), cl);
            messages.add(byteArrayOutputStream.toByteArray());
        }
        System.out.println("Starting deserialize");
        long start = System.currentTimeMillis();
        List<MyTestClass> res = new LinkedList<MyTestClass>();
        for (byte[] message : messages) {
            ByteArrayInputStream bis = new ByteArrayInputStream(message);
            res.add(kryo.readObject(new InputChunked(bis), MyTestClass.class));
        }
        System.out.println((System.currentTimeMillis() - start) + "ms");
    }
}
