import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.ThreadLocalRandom;

public class scratch {

    private static final String filepath = "/Users/craigkovar/Desktop/Demos/demo_framework/cb_demo_framework/work/dpz/data.json";
    private static final String[] segment = {"platinum", "gold", "silver", "bronze", "community"};
    private static final String[] fnames = {"craig", "mike", "steve", "joe", "cliff", "hiren", "edy", "nirvair", "justin", "ryan", "karthik", "jack"};
    private static final Integer size = 100000;

    public static void main(String[] args) {
        write_sample_data();
    }

    public static void write_sample_data() {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(filepath));

            for (int i = 0; i < size; i++) {
                bw.write(get_string()+"\n");
                bw.flush();
                if (i%1000 == 0) {
                    System.out.println("Processed :" + i + " records");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.flush();
                    bw.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static String get_string() {

        Integer segmentLoc = ThreadLocalRandom.current().nextInt(0, 5);
        Integer nameLoc = ThreadLocalRandom.current().nextInt(0, 12);
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"name\" : \"");
        sb.append(fnames[nameLoc]);
        sb.append("\", \"city\": \"chicago\", \"segment\" : \"");
        sb.append(segment[segmentLoc]);
        sb.append("\"}");

        return sb.toString();
    }

}


