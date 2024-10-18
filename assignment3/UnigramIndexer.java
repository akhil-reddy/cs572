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
        private Text doc_id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                FileSplit file_split = (FileSplit)context.getInputSplit();
                String filename = file_split.getPath().getName();
                String line = value.toString().toLowerCase();
                line = line.replaceAll("[^a-zA-Z\\s]", " ");

                StringTokenizer iter = new StringTokenizer(line);
                while (iter.hasMoreTokens()) {
                    word.set(iter.nextToken());
                    doc_id.set(filename + ":1");
                    context.write(word, doc_id);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private long reduce_rec = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                Map<String, Integer> counts = new HashMap<>();
                for (Text val : values) {
                    String[] parts = val.toString().split(":");
                    String doc_id = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    counts.put(doc_id, counts.getOrDefault(doc_id, 0) + count);
                    reduce_rec ++;
                }
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
                }
                result.set(sb.toString().trim());
                context.write(key, result);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.out.println("Unigram Indexer job completed. Status: " + result);

        System.exit(result ? 0 : 1);
    }
}