package org.apache.storm.container.tc;

import org.apache.storm.StormTimer;
import org.apache.storm.daemon.supervisor.DefaultUncaughtExceptionHandler;
import org.apache.storm.streams.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * class to get tc info and cache
 * provide the tc info to cgroupManager to control the bandwidth
 * <p>
 * support multi-NIC
 */
public class TcManager {

    private final static Logger LOG = LoggerFactory.getLogger(TcManager.class);

    private final static List<String> QDISC_SHOW_CMD = Arrays.asList("tc", "qdisc", "show");
    private final static List<String> CLASS_SHOW_CMD_PATTERN = Arrays.asList("tc", "class", "show", "dev");

    private final static TcManager manager = new TcManager();

    private final AtomicReference<List<TcQdisc>> cache;

    private final StormTimer refresher;

    private TcManager(){
        this.cache = new AtomicReference<>();
        refresh();
        this.refresher = new StormTimer(null, new DefaultUncaughtExceptionHandler());

        this.refresher.scheduleRecurring(60000, 60000, ()-> refresh());
    }

    public static TcManager getInstance() {
        return manager;
    }

    public void refresh(){
        try {
            this.cache.set(this.getTCInfo());
        }catch (Exception e){
            throw new RuntimeException("could not read tc info!");
        }
    }

    public List<TcQdisc> getCache(){
        return this.cache.get();
    }

    public List<TcQdisc> getTCInfo() throws IOException {

        List<TcQdisc> qdiscs;

        Pair<Integer, String> qdiscRet = exec(QDISC_SHOW_CMD, 2000);
        if (qdiscRet.getFirst() == 0) {
            String qdiscStr = qdiscRet.getSecond();
            qdiscs = TcQdisc.parse(qdiscStr);

            for (TcQdisc qdisc : qdiscs) {
                if (qdisc.isRoot()) {
                    List<String> cmd = new ArrayList<>(CLASS_SHOW_CMD_PATTERN);
                    cmd.add(qdisc.getNetworkCard());
                    Pair<Integer, String> classRet = exec(cmd, 2000);

                    if (classRet.getFirst() == 0) {
                        List<TcClass> classes = TcClass.parse(qdiscs, classRet.getSecond());
                        qdisc.setClasses(classes);
                    } else {
                        throw new RuntimeException("get tc qdisc error!");
                    }
                }
            }

            return qdiscs;

        } else {
            throw new RuntimeException("get tc qdisc error!");
        }
    }

    public static Pair<Integer, String> exec(List<String> command, final long timeout) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        final Process p = builder.start();

        Thread timeoutThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {

                }
                if (p != null) {
                    p.destroy();
                }
            }
        });

        timeoutThread.setDaemon(true);
        timeoutThread.start();

        InputStream in = null;
        StringBuilder sb = new StringBuilder(256);
        try {
            in = p.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }

            int exitVal = p.waitFor();
            return Pair.of(exitVal, sb.toString());
        } catch (Exception e) {
            p.destroy();
            throw new IOException("exe cmd " + command + " failed", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error("close InputStream err!", e);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(TcManager.getInstance().getTCInfo());
    }

}
