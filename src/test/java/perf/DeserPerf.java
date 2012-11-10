package perf;

import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Micro-benchmark for comparing performance of bean deserialization
 */
public final class DeserPerf extends PerfBase
{
    private final int REPS;

    private DeserPerf() {
        // Let's try to guestimate suitable size
        REPS = 9000;
    }
    
    public void test()
        throws Exception
    {
        int sum = 0;

        final MediaItem item = buildItem();
        
        // Use Jackson?
//        byte[] json = jsonMapper.writeValueAsBytes(item);
        byte[] avro =  avroWriter(MediaItem.class).writeValueAsBytes(item);
        
        System.out.println("Warmed up: data size is "+avro.length+" bytes; "+REPS+" reps -> "
                +((REPS * avro.length) >> 10)+" kB per iteration");
        System.out.println();

        final ObjectReader reader = avroReader(MediaItem.class);
        
        int round = 0;
        while (true) {
//            try {  Thread.sleep(100L); } catch (InterruptedException ie) { }
//            int round = 2;

            long curr = System.currentTimeMillis();
            String msg;
            round = (++round % 2);

//if (true) round = 3; 
            
            boolean lf = (round == 0);

            switch (round) {
            case 0:
            case 1:
                msg = "Deserialize, bind, Avro";
                sum += testDeser(reader, avro, REPS);
                break;

            default:
                throw new Error("Internal error");
            }

            curr = System.currentTimeMillis() - curr;
            if (lf) {
                System.out.println();
            }
            System.out.println("Test '"+msg+"' -> "+curr+" msecs ("
                               +(sum & 0xFF)+").");
        }
    }

    protected int testDeser(ObjectReader reader, byte[] input, int reps)
        throws Exception
    {
        MediaItem item = null;
        for (int i = 0; i < reps; ++i) {
            item = reader.readValue(input, 0, input.length);
        }
        return item.hashCode(); // just to get some non-optimizable number
    }
    
    public static void main(String[] args) throws Exception
    {
        new DeserPerf().test();
    }
}
