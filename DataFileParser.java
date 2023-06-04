import java.util.*;
import java.util.stream.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.sql.*;
import java.text.NumberFormat;


public class DataFileParser {

    private static String fileName;
    private static String directory;

    private static String timeStamp;
    private static String inputType;

    private enum InputType {FD, PD};


    public static void main(String[] args) {

        if (args.length > 1) {

            //fileName = args[1];
            //System.out.println("fileName: " + fileName);

            directory = args[1];
            System.out.println("Directory: " + directory);

            try {
                InputType iType = InputType.valueOf(args[0]);

                try {
                    List<File> files = Files.list(Paths.get(directory))
                        .map(Path::toFile)
                        .filter(File::isFile)
                        .collect(Collectors.toList());

                        //files.forEach(System.out::println);

                        files.forEach(fileTotalPath -> {
                            System.out.println("InputType: " + iType + " -> " + fileTotalPath);
                        } );
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            
                if (iType == InputType.FD) {
                    System.out.println("Input type is forbrugsdata.");

                    


                    /*
                    String[] filePathParts = fileName.split("/");
                    System.out.println("FileName: " + filePathParts[filePathParts.length - 1]);
                    
                    String myFileName = filePathParts[filePathParts.length - 1];

                    String[] fileNameParts = myFileName.split("[_.]");
                    System.out.println("string-parts: " + fileNameParts[0] +" - "+ fileNameParts[1] +" - "+ fileNameParts[2]);

                    timeStamp = fileNameParts[1];

                    new DataFileParser().go();
                    */
                    
                }
                else if (iType == InputType.PD) {
                    System.out.println("Input type is prisdata.");

                    //new DataFileParser().processPriceData();
                }
            } catch(IllegalArgumentException e) {
                System.out.println("Error: " + e.getMessage());
                System.exit(1);
            }


            /*

            String[] fileNameParts = fileName.split("[_.]");
            System.out.println("string-parts: " + fileNameParts[0] +" - "+ fileNameParts[1] +" - "+ fileNameParts[2]);

            timeStamp = fileNameParts[1];

            */
        }
        else {
            System.out.println("Please enter input type and a filename");
        }

        //new DataFileParser().go();
    }

    public void go() {

        try {
            //File myObj = new File("aura_20230505.txt");
            File myObj = new File(fileName);
            Scanner myReader = new Scanner(myObj);
            int lineCount = 0;

            //InsertElForbrugData("TEST", 1.99);

            while (myReader.hasNextLine()) {
              String data = myReader.nextLine();

              // skip first line. //
              if (lineCount > 0) {

                String[] splitArray = data.split(",");
                //String[] splitTimer = splitArray[0].split(".");
                //System.out.println(Integer.parseInt(splitArray[0].substring(1, 3)) + " - " + splitArray[1]);

                System.out.println(GetTimeStampOutput(timeStamp, Integer.parseInt(splitArray[0].substring(1, 3))) + " - " + splitArray[1]);
                //System.out.println(splitArray[0].substring(1, 3) + " - " + splitArray[1]);
                InsertElForbrugData(GetTimeStampOutput(timeStamp, Integer.parseInt(splitArray[0].substring(1, 3))), Double.parseDouble(splitArray[1]));
              }

              lineCount++;
              
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
    }

    public void processPriceData() {

        try {
            File myObj = new File(fileName);
            Scanner myReader = new Scanner(myObj);
            int lineCount = 0;

            while (myReader.hasNextLine()) {
              String data = myReader.nextLine();

              // skip first line. //
              if (lineCount > 0) {

                String[] splitArray = data.split(",");
                //String[] splitTimer = splitArray[0].split(".");
                System.out.println(splitArray[0].substring(1, splitArray[0].length() - 1) + " - " + splitArray[1].substring(1, splitArray[1].length() -1));
                InsertElPrisData(splitArray[0].substring(1, splitArray[0].length() - 1), Double.parseDouble(splitArray[1].substring(1, splitArray[1].length() -1)));
                //InsertElPrisData(splitArray[0],  Double.valueOf("22.33"));

                //System.out.println(GetTimeStampOutput(timeStamp, Integer.parseInt(splitArray[0].substring(1, 3))) + " - " + splitArray[1]);
                //System.out.println(splitArray[0].substring(1, 3) + " - " + splitArray[1]);
                //InsertElForbrugData(GetTimeStampOutput(timeStamp, Integer.parseInt(splitArray[0].substring(1, 3))), Double.parseDouble(splitArray[1]));
              }

              lineCount++;
              
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
    }

    private String GetTimeStampOutput(String myDate, int hour)
    {
        String timerStr = String.format("%02d", hour);
        String stamp = myDate + " " + timerStr + ":00:00"; 
        return stamp;
    }

    private void InsertElForbrugData(String stamp, double forbrug) 
    {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");  
            //Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/myTestdata","root","");  
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC","root","");  

            Statement stmt = con.createStatement();  

            String sql = "INSERT INTO elforbrugdata(time_stamp, forbrug) VALUES ('" + stamp + "'," + forbrug + ")";
            stmt.executeUpdate(sql);

            con.close();  
        }
        catch(Exception e)
        { 
            System.out.println(e);
        }  

    }

    private void InsertElPrisData(String stamp, double pris) 
    {
        //System.out.println("DATA: " + stamp + " - " + pris);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");  
            //Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/myTestdata","root","");  
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC","root","");  

            Statement stmt = con.createStatement();  

            String sql = "INSERT INTO elprisdata(time_stamp, pris) VALUES ('" + stamp + "'," + pris + ")";
            stmt.executeUpdate(sql);

            con.close();  
        }
        catch(Exception e)
        { 
            System.out.println(e);
        }  

    }

}