import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class Query3 {

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
            String age = record[2];
            String gender = record[3];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = C  + "," + age + "," + gender;
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

            //Make a custom value for the mapper to include where it came from
            String reduceValue = T +","+ transTotal;
            context.write(id, new Text(reduceValue));
        }
    }

    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {
        //Keeps track of what dataset it came from
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Only get what is needed
            int age = Integer.parseInt(record[0]);
            String age_S = getAgeGroup(age);
            String gender = record[1];

            String combinedKey =age_S + "," + gender;
            id.set(combinedKey);

            String min = record[2];
            String max = record[3];
            String total = record[4];
            String count = record[5];

            //Make a custom value for the mapper to include where it came from
            String reduceValue = min + "," + max + "," + total + "," + count;
            context.write(id, new Text(reduceValue));

        }
    }

    public static class JoinReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Variables for the final output
            float minTotal = 10000;
            float maxTotal = 0;
            float transTotalSum = 0;
            int count = 0;
            String age = "";
            String gender = "";

            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");

                //Data set it came from
                String dataset = record[0];

                //Check if we got the customer or transaction
                //Should only get one customer
                if (dataset.equals("C")) {
                    //Extract the name and salary of customer
                    age = record[1];
                    gender = record[2];
                } else if (dataset.equals("T")) {

                    //Determine the min and max transaction for the customer
                    float transTotal = Float.parseFloat(record[1]);
                    transTotalSum += transTotal;
                    count++;
                    if (transTotal > maxTotal){
                        maxTotal = transTotal;
                    }
                    if (transTotal < minTotal){
                        minTotal = transTotal;
                    }
                }
            }
            //Create custom value for output
            // <null, age + gender + min + max + total + count>
            String result = age + "," + gender + "," + minTotal + "," + maxTotal + "," + transTotalSum+ "," + count;
            context.write(null, new Text(result));
        }
    }

    public static class AgeReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Variables for the final output
            float minTotal = 10000;
            float maxTotal = 0;
            float transTotalSum = 0;
            int count = 0;
            String keyString = key.toString();
            String[] keys = keyString.split(",");
            String age = keys[0];
            String gender = keys[1];

            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");


                float minCustTotal = Float.parseFloat(record[0]);
                float maxCustTotal = Float.parseFloat(record[1]);

                if (minCustTotal < minTotal){
                    minTotal = minCustTotal;
                }

                if (maxCustTotal > maxTotal){
                    maxTotal = maxCustTotal;
                }

                transTotalSum +=  Float.parseFloat(record[2]);
                count += Integer.parseInt(record[3]);

            }

            //Calculate average
            float averageTransTotal = transTotalSum / count;
            //Create custom value for output
            // <null, age + gender + min + max + total + count>
            String result = age + "," + gender + "," + minTotal + "," + maxTotal + "," + averageTransTotal;
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query 3 Join");
        job.setJarByClass(Query3.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("query3FirstOutput"));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Query 3 Aggregate");
        job2.setJarByClass(Query3.class);
        job2.setReducerClass(AgeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job2, new Path("query3FirstOutput"), TextInputFormat.class, AgeMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static String getAgeGroup(int age) {
        if (age < 20) {
            return "[10-20)";
        } else if (age < 30) {
            return "[20-30)";
        } else if (age < 40) {
            return "[30-40)";
        } else if (age < 50) {
            return "[40-50)";
        } else if (age < 60) {
            return "[50-60)";
        } else if (age <= 70) {
            return "[60-70]";
        } else {
            return "COULD NOT GROUP";
        }
    }
}
