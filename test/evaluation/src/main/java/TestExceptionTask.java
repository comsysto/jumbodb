import java.util.concurrent.Callable;

/**
 * @author Carsten Hufe
 */
public class TestExceptionTask implements Runnable {
    @Override
    public void run() {
        Integer a = null;
        a.equals(2);
    }
}
