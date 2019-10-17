import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.sql.SQLException;

import java.sql.ResultSet;

public class multithread {
	static int var1=0;


	public static String driver="org.apache.hadoop.hive.jdbc.HiveDriver";
    public static class Thread1 implements Runnable {
    	
        public void run() {
        	try
            {
                System.out.println("started");
                Class.forName(driver);
               
               
            }
            catch(ClassNotFoundException e)
            {
                e.printStackTrace();
            }
            try {
            	Connection conn=DriverManager.getConnection("jdbc:hive://localhost:10000/guwati","","");
                Statement stmt=conn.createStatement();

                //stmt.executeQuery("select * from new1");
               
                String sql2="select * from guwati";
                System.out.println("running : "+sql2);
                ResultSet res1=stmt.executeQuery(sql2);
                while(res1.next())
                {
                    //System.out.println(res1.getString(1) + " \t" + res1.getInt(2));
                
                if(res1.getString(1).equals("TE00ST0002"))
                {
                	var1+=res1.getInt(2);
                }
               
                }
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Thread2 implements Runnable {
 
        public void run() {
        	try
            {
                System.out.println("started");
                Class.forName(driver);
               
               
            }
            catch(ClassNotFoundException e)
            {
                e.printStackTrace();
            }
            try {
            	Connection conn=DriverManager.getConnection("jdbc:hive://localhost:10000/nashik","","");


                Statement statment = conn.createStatement();

                
                String sql2="select * from nashik";
                System.out.println("running : "+sql2);
                ResultSet res1=statment.executeQuery(sql2);
                while(res1.next())
                {
                    //System.out.println(res1.getString(1) + " \t" + res1.getInt(2));
                
                if(res1.getString(1).equals("TE00ST0002"))
                {
                	var1+=res1.getInt(2);
                }
                
                }
                System.out.println("Total fee is : "+ var1);
             

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        Thread thread1 = new Thread(new Thread1());
        Thread thread2 = new Thread(new Thread2());

        thread1.start();
        thread2.start();

        try {
            thread1.join(1000000);
            thread2.join(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


