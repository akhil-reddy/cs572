import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String filename = fileSplit.getPath().getName();
                String line = value.toString().toLowerCase();
                line = line.replaceAll("[^a-zA-Z\\s]", " ");

                String[] words = line.split("\\s+", 1000);
                for (int i = 0; i < words.length - 1; i++) {
                    String bigramStr = words[i] + " " + words[i + 1];
                    if (selectedBigrams.contains(bigramStr)) {
                        System.out.println("Has bigram: " + bigramStr);  // Debug statement
                        bigram.set(bigramStr);
                        docId.set(filename + ":1");
                        context.write(bigram, docId);
                        System.out.println("Emitting: " + bigram + " -> " + docId);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> counts = new HashMap<>();
            for (Text val : values) {
                String[] docCounts = val.toString().split(" ");
                for (String docCount : docCounts) {
                    String[] parts = docCount.split(":");
                    String docId = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    counts.put(docId, counts.getOrDefault(docId, 0) + count);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
            }
            result.set(sb.toString().trim());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bigram indexer");
        job.setJarByClass(BigramIndexer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.out.println("Bigram Indexer job completed. Status: " + result);

        System.exit(result ? 0 : 1);
    }
}