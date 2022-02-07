import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class TopKCommonWords {

    public static class WordToDocumentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable isInput1 = new IntWritable(0);
        private final static IntWritable notInput1 = new IntWritable(1);
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
                // @TODO update the file link -> get from the terminal input
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
            IntWritable val = (documentName.equals("task1-input1.txt")) ? isInput1 : notInput1;
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            String currWord;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                if (!stopWords.contains(currWord)) {
                    word.set(currWord);
                    context.write(word, val);
                }
            }
        }
    }

    public static class WordToDocumentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Hi i'm inside reduce");
            IntWritable maxOfCount = new IntWritable();
            int firstDocCount = 0;
            int secondDocCount = 0;
            boolean inDocOne = false;
            boolean inDocTwo = false;
            for (IntWritable val: values) {
                if (val.get() == 0) {
                    inDocOne = true;
                    firstDocCount += 1;
                } else {
                    inDocTwo = true;
                    secondDocCount += 1;
                }
            }
            if (inDocOne && inDocTwo) {
                int maxDocCount = Math.max(firstDocCount, secondDocCount);
                maxOfCount.set(maxDocCount);
                context.write(key, maxOfCount);
            }
        }
    }

    public static class CountToWordMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text word = new Text();
            IntWritable wordCount = new IntWritable();
            StringTokenizer itr = new StringTokenizer(value.toString());
            String currWord;
            int currCount;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                // @TODO try catch in notnumberexception
                currCount = Integer.parseInt(itr.nextToken());
                System.out.println("This is current word >> " + currWord);
                word.set(currWord);
                wordCount.set(currCount);
                context.write(wordCount, word);
            }
        }
    }

    public static class CountToWordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable intWritable, Text text, Context context) throws IOException, InterruptedException {
            context.write(intWritable, text);
        }
    }

    public static void main(String[] args) throws Exception {

        String outputTempDir = "D:\\NUSY4S2\\BigDataProj\\WordCount\\temp";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopKCommonWords");
        job.setJarByClass(TopKCommonWords.class);
        job.setMapperClass(WordToDocumentMapper.class);
        job.setReducerClass(WordToDocumentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[2]).toUri());
        FileOutputFormat.setOutputPath(job, new Path(outputTempDir));

        boolean success = job.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }
        Job job2 = Job.getInstance(conf, "SecondJob");
//        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(CountToWordMapper.class);
        job2.setReducerClass(CountToWordReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(outputTempDir));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        success = job2.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }
        System.exit(0);
    }
}
