import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UnigramIndexer {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docId = new Text();
        private long mapInputRecords = 0;

        @Override
        protected void setup(Context context) {
            System.out.println("Mapper setup complete");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                FileSplit fileSplit = (FileSplit)context.getInputSplit();
                String filename = fileSplit.getPath().getName();
                String line = value.toString().toLowerCase();
                line = line.replaceAll("[^a-zA-Z\\s]", " ");
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    docId.set(filename + ":1");
                    context.write(word, docId);
                    mapInputRecords++;
                }
            } catch (Exception e) {
                System.err.println("Error in mapper: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Mapper processed " + mapInputRecords + " records");
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
                    reduceInputRecords++;
                }
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
                }
                result.set(sb.toString().trim());
                context.write(key, result);
                System.out.println("Reducer output: " + key + " -> " + result);
            } catch (Exception e) {
                System.err.println("Error in reducer: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Reducer processed " + reduceInputRecords + " records");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unigram indexer");
        job.setJarByClass(UnigramIndexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);  // Adjust this based on your data size

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Starting Unigram Indexer job...");
        System.out.println("Input path: " + args[0]);
        System.out.println("Output path: " + args[1]);

        boolean result = job.waitForCompletion(true);
        System.out.println("Unigram Indexer job completed. Result: " + result);

        System.exit(result ? 0 : 1);
    }
}