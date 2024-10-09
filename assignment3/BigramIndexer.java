import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramIndexer {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text bigram = new Text();
        private Text docId = new Text();
        private Set<String> selectedBigrams = new HashSet<>(Arrays.asList(
                "computer science", "information retrieval", "power politics",
                "los angeles", "bruce willis"));
        private long mapInputRecords = 0;
        private long bigramsEmitted = 0;

        @Override
        protected void setup(Context context) {
            System.out.println("Mapper setup complete. Selected bigrams: " + selectedBigrams);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String filename = fileSplit.getPath().getName();

                String line = value.toString().toLowerCase();
                line = line.replaceAll("[^a-zA-Z\\s]", " ");

                String[] words = line.split("\\s+");
                for (int i = 0; i < words.length - 1; i++) {
                    String bigramStr = words[i] + " " + words[i + 1];
                    if (selectedBigrams.contains(bigramStr)) {
                        bigram.set(bigramStr);
                        docId.set(filename + ":1");
                        context.write(bigram, docId);
                        bigramsEmitted++;
                    }
                }
                mapInputRecords++;
                if (mapInputRecords % 1000 == 0) {
                    System.out.println(
                            "Processed " + mapInputRecords + " records, emitted " + bigramsEmitted + " bigrams");
                }
            } catch (Exception e) {
                System.err.println("Error in mapper: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) {
            System.out.println(
                    "Mapper processed " + mapInputRecords + " records, emitted " + bigramsEmitted + " bigrams");
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private long reduceInputRecords = 0;

        @Override
        protected void setup(Context context) {
            System.out.println("Reducer setup complete");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                Map<String, Integer> counts = new HashMap<>();

                for (Text val : values) {
                    String[] parts = val.toString().split(":");
                    String docId = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    counts.put(docId, counts.getOrDefault(docId, 0) + count);
                }

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
                }
                result.set(sb.toString().trim());
                context.write(key, result);
                reduceInputRecords++;
                if (reduceInputRecords % 100 == 0) {
                    System.out.println("Reduced " + reduceInputRecords + " bigrams");
                }
            } catch (Exception e) {
                System.err.println("Error in reducer: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) {
            System.out.println("Reducer processed " + reduceInputRecords + " bigrams");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bigram indexer");
        job.setJarByClass(BigramIndexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); // Adjust based on your needs

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Starting Bigram Indexer job...");
        System.out.println("Input path: " + args[0]);
        System.out.println("Output path: " + args[1]);

        boolean result = job.waitForCompletion(true);
        System.out.println("Bigram Indexer job completed. Result: " + result);
        System.exit(result ? 0 : 1);
    }
}