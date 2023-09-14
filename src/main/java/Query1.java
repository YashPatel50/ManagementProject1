import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {

    public static class CustomerMapper
            extends Mapper<Object, Text, Text, Text> {
        //Keeps track of what dataset it came from
        private final static String C = "C";
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Only get what is needed
            id.set(record[0]);
            String name = record[1];
            String salary = record[5];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = C + "," + name + "," + salary;
            context.write(id, new Text(reduceValue));

        }
    }

    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text>{
        //Keeps track of what dataset it came from
        private final static String T = "T";
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Only get what is needed
            id.set(record[1]);
            String transTotal = record[2];
            String transNum = record[3];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = T +","+ transTotal+"," + transNum;
            context.write(id, new Text(reduceValue));
        }
    }

    public static class TotalReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Variables for the final output
            int count = 0;
            float totalSum = 0;
            int minItems = 1000;
            String name = "";
            String salary = "";

            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");

                //Data set it came from
                String dataset = record[0];

                //Check if we got the customer or transaction
                //Should only get one customer
                if (dataset.equals("C")) {
                    //Extract the name and salary of customer
                    name = record[1];
                    salary = record[2];
                } else if (dataset.equals("T")) {
                    //Increment count as we have one more transaction
                    count++;
                    //Update the current total
                    totalSum += Float.parseFloat(record[1]);
                    //Check if this is a new minimum number of items
                    int numItems = Integer.parseInt(record[2]);
                    if (numItems < minItems) {
                        minItems = numItems;
                    }
                }
            }
            //Create custom value for output
            String result = key.toString() + "," + name + "," + salary+ "," + count+ "," + totalSum+ "," + minItems;
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query 1");
        job.setJarByClass(Query1.class);
        job.setReducerClass(TotalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
