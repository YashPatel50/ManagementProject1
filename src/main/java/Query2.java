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

public class Query2 {

    public static class CustomerMapper
            extends Mapper<Object, Text, Text, Text> {
        //Keeps track of which dataset it came from
        private final static String C = "C";
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Only get what is needed
            id.set(record[0]);
            String countryCode = record[4];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = C + "," + countryCode;
            // <id, C + country code >
            context.write(id, new Text(reduceValue));

        }
    }

    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text>{

        //Keeps track of which dataset it came from
        private final static String T = "T";
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Only get what is needed
            id.set(record[1]);
            String transTotal = record[2];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = T +","+ transTotal;
            //<id , T + transtotal>
            context.write(id, new Text(reduceValue));
        }
    }

    public static class CountryMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text countryCode = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Get the country code
            countryCode.set(record[1]);

            //Send the value as is no need to remove countrycode from value
            // <countryCode, ID+CountryCode+MinTotal+MaxTotal>
            context.write(countryCode, value);
        }
    }

    public static class TotalReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //We need to keep track of the total for the customer
            float minTotal = 10000;
            float maxTotal = 0;
            String countryCode = "";

            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");

                //Data set it came from
                String dataset = record[0];
                if (dataset.equals("C")) {
                    //Get the country code of customer
                    countryCode = record[1];
                } else if (dataset.equals("T")) {
                    //Determine the min and max transaction for the customer
                    float transTotal = Float.parseFloat(record[1]);
                    if (transTotal > maxTotal){
                        maxTotal = transTotal;
                    }
                    if (transTotal < minTotal){
                        minTotal = transTotal;
                    }
                }
            }
            String result = key.toString() + "," + countryCode + "," + minTotal + "," + maxTotal;
            // <null, ID+CountryCode+MinTotal+MaxTotal>
            context.write(null, new Text(result));
        }
    }

    public static class CountryReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //We need to keep track of the total for the country code
            float minTotal = 10000;
            float maxTotal = 0;
            int numCustomers = 0;

            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");

                //Increment the count
                numCustomers++;

                float minCustTotal = Float.parseFloat(record[2]);
                float maxCustTotal = Float.parseFloat(record[3]);

                if (minCustTotal < minTotal){
                    minTotal = minCustTotal;
                }

                if (maxCustTotal > maxTotal){
                    maxTotal = maxCustTotal;
                }
            }
            String result = key.toString() + "," + numCustomers + "," + minTotal + "," + maxTotal;
            // <null, CountryCode+NumberOfCustomers+MinTransTotal+MaxTransTotal>
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query 2 Join");
        job.setJarByClass(Query2.class);
        job.setReducerClass(TotalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("query2FirstOutput"));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Query 2 Join");
        job2.setJarByClass(Query2.class);
        job2.setReducerClass(CountryReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job2, new Path("query2FirstOutput"), TextInputFormat.class, CountryMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

