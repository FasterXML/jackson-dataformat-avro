package perf;

import java.io.*;

import com.fasterxml.jackson.databind.ObjectWriter;

public final class SerPerf extends PerfBase
{
    /*
    /**********************************************************
    /* Actual test
    /**********************************************************
     */

    private final int REPS;

    private SerPerf() throws Exception
    {
        // Let's try to guesstimate suitable size...
        REPS = 6000;
    }
    
    public void test()
        throws Exception
    {
        int i = 0;
        int sum = 0;

        ByteArrayOutputStream result = new ByteArrayOutputStream();

        final MediaItem item = buildItem();
        final ObjectWriter writer = avroWriter(MediaItem.class);
        
        while (true) {
//            Thread.sleep(150L);
            ++i;
            int round = (i % 3);

            // override?
            round = 0;

            long curr = System.currentTimeMillis();
            String msg;

            switch (round) {

            case 0:
                msg = "Serialize, Avro";
                sum += testObjectSer(writer, item, REPS+REPS, result);
                break;
            default:
                throw new Error("Internal error");
            }

            curr = System.currentTimeMillis() - curr;
            if (round == 0) {  System.out.println(); }
            System.out.println("Test '"+msg+"' -> "+curr+" msecs ("+(sum & 0xFF)+").");
            if ((i & 0x1F) == 0) { // GC every 64 rounds
                System.out.println("[GC]");
                Thread.sleep(20L);
                System.gc();
                Thread.sleep(20L);
            }
        }
    }

    private int testObjectSer(ObjectWriter writer, Object value, int reps,
            ByteArrayOutputStream result)
        throws Exception
    {
        for (int i = 0; i < reps; ++i) {
            result.reset();
            writer.writeValue(result, value);
        }
        return result.size(); // just to get some non-optimizable number
    }
    
    public static void main(String[] args) throws Exception
    {
        new SerPerf().test();
    }
}
