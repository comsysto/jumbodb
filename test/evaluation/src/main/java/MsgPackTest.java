import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by carsten on 24/09/14.
 */
public class MsgPackTest {
    public static void main(String[] args) throws IOException {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", RandomStringUtils.randomAlphanumeric(100));
        data.put("test2", RandomStringUtils.randomAlphanumeric(100));
        data.put("test3", RandomStringUtils.randomAlphanumeric(100));
        data.put("test4", RandomStringUtils.randomAlphanumeric(100));
        data.put("test5", RandomStringUtils.randomAlphanumeric(100));
        Map<String, Object> subData = new HashMap<String, Object>();
        subData.put("test1", RandomStringUtils.randomAlphanumeric(100));
        subData.put("test2", RandomStringUtils.randomAlphanumeric(100));
        subData.put("test3", RandomStringUtils.randomAlphanumeric(100));
        subData.put("do", new Random().nextDouble());
        subData.put("in", new Random().nextInt());
        subData.put("lon", new Random().nextLong());
        subData.put("test3", RandomStringUtils.randomAlphanumeric(100));
        subData.put("test3", RandomStringUtils.randomAlphanumeric(100));
        data.put("sub", subData);

        long start = System.currentTimeMillis();
        ObjectMapper om = new ObjectMapper();
        byte[] bytes = om.writeValueAsBytes(data);
        System.out.println(bytes.length);
        for(int i = 0; i < 10000000; i++) {
            Map map = om.readValue(bytes, Map.class);
            map.size();
        }
        System.out.println("Jackson: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        MessagePack msgpack = new MessagePack();
        bytes = msgpack.write(data);
        System.out.println(bytes.length);
        for(int i = 0; i < 10000000; i++) {
            Value map = msgpack.read(bytes);
            MapValue mapValue = map.asMapValue();
            mapValue.size();
        }
        System.out.println("MsgPack: " + (System.currentTimeMillis() - start));

    }
}
