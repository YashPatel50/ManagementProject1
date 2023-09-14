import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Datasets {

    public static void main(String[] args) {
        createCustomersDataset();
        createTransactionsDataset();
    }


    private static void createCustomersDataset(){

        try{
            FileWriter writer = new FileWriter("customers");
            //Iterate from 1 to 50,000
            for (int i = 1; i <= 50000; i++) {
                //Create each of the attributes
                String id_s = String.valueOf(i);
                String name = generateRandomString(10, 20);
                String age = String.valueOf(generateRandomInteger(10, 70));
                String gender = generateRandomGender();
                String countryCode = String.valueOf(generateRandomInteger(1, 10));
                String salary = String.valueOf(generateRandomFloat(100, 10000));
                String record = id_s + "," + name + "," + age + "," + gender + "," + countryCode + "," + salary + "\n";
                writer.write(record);
            }
            writer.close();

        } catch (IOException e){
            System.out.println("An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }

    private static void createTransactionsDataset(){
        try{
            FileWriter writer = new FileWriter("transactions");
            //Iterate from 1 to 5,000,000
            for (int i = 1; i <= 5000000; i++) {
                //Create each of the attributes
                String transID = String.valueOf(i);
                String custID = String.valueOf(generateRandomInteger(1, 50000));
                String transTotal = String.valueOf(generateRandomFloat(10, 1000));
                String transNumItems = String.valueOf(generateRandomInteger(1, 10));
                String transDesc = generateRandomString(20, 50);
                String record = transID + "," + custID + "," + transTotal + "," + transNumItems + "," + transDesc + "\n";
                writer.write(record);
            }
            writer.close();

        } catch (IOException e){
            System.out.println("An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }


    private static String generateRandomString(int min, int max) {
        //Ensures min is less than max
        if (min >= max) {
            throw new IllegalArgumentException("Max must be greater than min");
        }
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = (int)(Math.random() * (max - min + 1) + 10); // Determine the length
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)(random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }

    private static int generateRandomInteger(int min, int max) {
        //Ensures min is less than max
        if (min >= max) {
            throw new IllegalArgumentException("Max must be greater than min");
        }
        Random random = new Random();
        return random.nextInt((max - min) + 1) + min;
    }

    private static float generateRandomFloat(float min, float max) {
        //Ensures min is less than max
        if (min >= max) {
            throw new IllegalArgumentException("Max must be greater than min");
        }
        Random random = new Random();
        return random.nextFloat() * (max - min) + min;
    }

    private static String generateRandomGender(){
        Random random = new Random();
        if (random.nextBoolean()){
            return "Male";
        } else {
            return "Female";
        }
    }
}
