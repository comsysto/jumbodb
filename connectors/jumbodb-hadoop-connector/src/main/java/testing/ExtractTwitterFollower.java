package testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Map;

/**
 * Created by Carsten on 22.09.2014.
 */
public class ExtractTwitterFollower {

    public static void main(String[] args) throws IOException {
        ObjectMapper om = new ObjectMapper();
        FileInputStream fis = new FileInputStream("C:\\Development\\data\\twitter\\input_big\\sample-0002.json");
        BufferedInputStream bis = new BufferedInputStream(fis);
        BufferedReader br = new BufferedReader(new InputStreamReader(bis, "UTF-8"));

        FileOutputStream fos = new FileOutputStream("C:\\Development\\data\\twitter_followers_count_big\\followers.txt");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(bos));

        long count = 0;
        String line;
        while ((line = br.readLine()) != null) {
            Map<String, Object> map = om.readValue(line, Map.class);
            Map<String, Object> user = (Map<String, Object>) map.get("user");
            if(user != null) {
                Integer followers_count = (Integer) user.get("followers_count");
                if(followers_count != null) {
                    bw.write(followers_count.toString());
                    bw.newLine();
                }
            }

            if(count % 1000 == 0) {
                System.out.println(count);
            }
            count++;
        }

        IOUtils.closeQuietly(bw);
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(fos);

        IOUtils.closeQuietly(br);
        IOUtils.closeQuietly(bis);
        IOUtils.closeQuietly(fis);
    }
}
