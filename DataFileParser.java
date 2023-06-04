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
    
    private static String directory;
    private enum InputType {CD, PD};

    public static void main(String[] args) {

        if (args.length > 1) {

            directory = args[1];
            System.out.println("Directory: " + directory);

            try {
                InputType iType = InputType.valueOf(args[0]);

                try {
                    List<File> files = Files.list(Paths.get(directory))
                        .map(Path::toFile)
                        .filter(File::isFile)
                        .sorted()
                        .collect(Collectors.toList());

                        files.forEach(fileTotalPath -> {
                            System.out.println("InputType: " + iType + " -> " + fileTotalPath);

                            if (iType == InputType.CD) {
                                System.out.println("Input type is consumption data.");
                                new DataFileParser().processConsumptionData(fileTotalPath.toString());
                            }
                            else if (iType == InputType.PD) {
                                System.out.println("Input type is price data.");
                                new DataFileParser().processPriceData(fileTotalPath.toString());
                            }

                        });
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            } catch(IllegalArgumentException e) {
                System.out.println("Error: " + e.getMessage());
                System.exit(1);
            }
        }
        else {
            System.out.println("Please enter input type and a filename");
        }
    }

    public void processConsumptionData(String filePath) {

        try {

            String[] filePathParts = filePath.split("/");
            String myFileName = filePathParts[filePathParts.length - 1];
            System.out.println("FileName: " + myFileName);

            String[] fileNameParts = myFileName.split("[_.]");
            System.out.println("string-parts: " + fileNameParts[0] +" - "+ fileNameParts[1] +" - "+ fileNameParts[2]);

            String timeStampFromName = fileNameParts[1];
            
            File myObj = new File(filePath);
            Scanner myReader = new Scanner(myObj);
            int lineCount = 0;

            while (myReader.hasNextLine()) {
              String data = myReader.nextLine();

              // skip first line. //
              if (lineCount > 0) {

                String[] splitArray = data.split(",");
                System.out.println(GetTimeStampOutput(timeStampFromName, Integer.parseInt(splitArray[0].substring(1, 3))) + " - " + splitArray[1]);
                InsertElForbrugData(GetTimeStampOutput(timeStampFromName, Integer.parseInt(splitArray[0].substring(1, 3))), Double.parseDouble(splitArray[1]));
              }
              lineCount++;
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
    }

    public void processPriceData(String filePath) {

        try {
            File myObj = new File(filePath);
            Scanner myReader = new Scanner(myObj);
            int lineCount = 0;

            while (myReader.hasNextLine()) {
              String data = myReader.nextLine();

              // skip first line. //
              if (lineCount > 0) {

                String[] splitArray = data.split(",");
                System.out.println(splitArray[0].substring(1, splitArray[0].length() - 1) + " - " + splitArray[1].substring(1, splitArray[1].length() -1));
                InsertElPrisData(splitArray[0].substring(1, splitArray[0].length() - 1), Double.parseDouble(splitArray[1].substring(1, splitArray[1].length() -1)));
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