package demojdbc;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
    Loading drivers
Getting a database connection (create instance of the class Connection)‏
Create a database query (create instance of the class (Statement, PreparedStatement, CallableStatement)‏
Execution request (for example, call  method execute(), executeQuery(), executeUpdate() class Statement)‏
Processing query result (например, обработка данных, содержащихся в class ResultSet)‏
Clear request (call  method close() class Statement, PreparedStatement or CallableStatement)‏
Close connection (call  method close() class Connection)‏
 */
public class DemoJDBC {

    /*
    1. Connect to DB
    2. Create tables
    3. Read data from thw fiel
    4. write data in tables
    5. executind select
     */
    public static void main(String[] args) {

        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found " + e);
        }

        try (Connection con = DriverManager.getConnection("jdbc:derby://localhost:1527/testDb", "username",
                "password");) {

            try {
                Statement st = con.createStatement();
                String table = "CREATE TABLE Users(id integer primary key, login char (8) unique)";
                st.executeUpdate(table);
                System.out.println("Table USERS creation process successfully!");
                table = "CREATE TABLE REGISTRATION(code integer primary key,"
                        + "id integer foreign key references Users(id),role varchar(16),date timestamp)";
                st.executeUpdate(table);
                System.out.println("Table Registration creation process successfully!");
            } catch (SQLException s) {
                System.out.println("Table all ready exists!" + s);
            }
            
            File file = new File("../../../data.log");
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            String str = "";
            int bufferSize = 1024;
            byte[] buffer = new byte[bufferSize];
            int rd;
            while ((rd = in.read(buffer)) > -1)
            {
                str += new String(buffer, 0, rd);
            }
            String[] words = str.split("\\s+");
            int splinSize =4;
   

         PreparedStatement pstmt = con.prepareStatement("UPDATE USERS SET ID = ?, LOGIN = ?");
         for (int i=0; i<words.length/splinSize; i++)
         {
              try{
            pstmt.setInt(1, i+1);
            pstmt.setString(2, words[i*splinSize]);
            pstmt.executeUpdate();
              }
              catch (Exception ex){Logger.getLogger(DemoJDBC.class.getName()).log(Level.SEVERE, null, ex);}
         }

            
         
         pstmt = con.prepareStatement("UPDATE REGISTRATION SET code = ?, id = ?, role = ?, date = ?");
         for (int i=0; i<words.length/splinSize; i++)
         {
              try{
            pstmt.setInt(1, i+1);
            pstmt.setInt(2, i+1);
            pstmt.setString(3, words[i*splinSize + 1]);
            pstmt.setDate(4, new java.sql.Date(0));
            pstmt.executeUpdate();
              }
              catch (Exception ex){Logger.getLogger(DemoJDBC.class.getName()).log(Level.SEVERE, null, ex);}
         }

         
         Statement st = con.createStatement();
         String select = "SELECT * FROM REGISTRATION";
         ResultSet rs = st.executeQuery(select);
                 
            
        } catch (SQLException e) {
            System.out.println("SQL exception occured" + e);
        }finally {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(DemoJDBC.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        }

        
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DemoJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | SQLException ex) {
            Logger.getLogger(DemoJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
}
