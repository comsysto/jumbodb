import org.xerial.snappy.Snappy;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class Other {
    public static void main(String[] args) {
        System.out.println(Snappy.maxCompressedLength(512 * 1024 * 1024));
    }
}
