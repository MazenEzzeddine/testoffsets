import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);
    // static BinPackRestructure bp;




    public static void main(String[] args) throws ExecutionException, InterruptedException {
        initialize();
    }


    private static void initialize() throws InterruptedException, ExecutionException {


        Lag.readEnvAndCrateAdminClient();
        log.info("Warming 15  seconds.");
        Thread.sleep(15 * 1000);



        while (true) {
            log.info("Querying Prometheus");
            ArrivalProducer.callForArrivals();
            Lag.getCommittedLatestOffsetsAndLag2();

            //Lag.getCommittedLatestOffsetsAndLag3();

            log.info("--------------------");
            log.info("--------------------");
            log.info("Sleeping for 500 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(1000);

        }
    }


















}
