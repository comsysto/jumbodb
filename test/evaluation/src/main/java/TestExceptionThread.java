import java.util.concurrent.*;

/**
 * @author Carsten Hufe
 */
public class TestExceptionThread {
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public TestExceptionThread() {
        Future<?> submit = executorService.submit(new TestExceptionTask());
        try {
            Object o = submit.get();
            System.out.println(o);
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public static void main(String[] args) {
        new TestExceptionThread();

    }
}
