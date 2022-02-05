import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class TopKCommonWords {

    public static class WordToDocumentMapper extends Mapper<Object, Text, Text, BooleanWritable> {
        private final static BooleanWritable isInput1 = new BooleanWritable(true);
        private final static BooleanWritable notInput1 = new BooleanWritable(false);
        private Set<String> stopWords = new HashSet<String>();
        private ArrayList<Integer> mList = new ArrayList<>();

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            // get all the stop words over here
            System.out.println("I'm inside the set up");
            URI[] localpaths = context.getCacheFiles();
            for(URI uri: localpaths) {
                System.out.println("Cahcefile ==> " + uri.getPath());
            }
            try {
                File stopwordsFile = new File("D:\\NUSY4S2\\BigDataProj\\WordCount\\input2\\stopwords.txt../../../input2/stopwords.txt");
                System.out.println("File path: " + stopwordsFile.getAbsolutePath());
                BufferedReader bufferedReader = new BufferedReader(new FileReader(stopwordsFile));
                String stopword;
                while ((stopword = bufferedReader.readLine()) != null) {
                    stopWords.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception occurred during parsing stopwords file: " + StringUtils.stringifyException(e));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String documentName = ((FileSplit)context.getInputSplit()).getPath().getName();
            System.out.println("Document name: " + documentName);
            BooleanWritable bool = (documentName.equals("task1-input1.txt")) ? isInput1 : notInput1;
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            String currWord;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                if (!stopWords.contains(currWord)) {
                    word.set(currWord);
                    context.write(word, bool);
                }
            }
        }
    }

    public static class WordToDocumentReducer extends Reducer<Text, BooleanWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Hi i'm inside reduce");
            IntWritable firstDocCountWritable = new IntWritable();
            IntWritable secondDocCountWritable = new IntWritable();
            int firstDocCount = 0;
            int secondDocCount = 0;
            for (BooleanWritable val: values) {
                if (val.get()) {
                    firstDocCount += 1;
                } else {
                    secondDocCount += 1;
                }
            }
            firstDocCountWritable.set(firstDocCount);
            secondDocCountWritable.set(secondDocCount);
            context.write(key, firstDocCountWritable);
            context.write(key, secondDocCountWritable);
        }
    }

    public static class CountToWordMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
        public void map(Text text, IntWritable intWritable, Context context) throws IOException, InterruptedException {
            context.write(intWritable, text);
        }
    }

    public static class CountToWordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable intWritable, Text text, Context context) throws IOException, InterruptedException {
            context.write(intWritable, text);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopKCommonWords");
        job.setJarByClass(TopKCommonWords.class);
        job.setMapperClass(WordToDocumentMapper.class);
        job.setReducerClass(WordToDocumentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BooleanWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[2]).toUri());

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "SecondJob");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapperClass(CountToWordMapper.class);
        job2.setReducerClass(CountToWordReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(BooleanWritable.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0: 1);

    }
}
