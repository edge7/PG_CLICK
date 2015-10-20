import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Scanner;

class Tomcat_threads extends Thread{

    public Tomcat_threads() {

    }

    public void run(){

        BufferedReader br = null;

        int counter = 0;
        try {

            br = new BufferedReader(new FileReader("/Users/Elena/Documents/Reply/7request_sec/nonreg.csv.csv"));

            String line;
            long start;
            while( (line = br.readLine()) != null)
            {
                //if( counter_tot == 2) break;
                String des_cosa = line.split(",")[1];
                String cd_visitatore = line.split(",")[0];

                String content = null;
                String url = "";
                URLConnection connection = null;
                try {
                    url = "http://54.154.233.250:8080/main.jsp?des_cosa=" ;
                    des_cosa = URLEncoder.encode(des_cosa, "UTF-8");
                    String suffix =  des_cosa + "&id=" + cd_visitatore;
                    //System.out.println(url);
                    //suffix = URLEncoder.encode(suffix, "UTF-8");
                    url = url  + suffix;
                    System.out.println(url);
                    start = System.currentTimeMillis();
                    connection =  new URL(url).openConnection();
                    Scanner scanner = new Scanner(connection.getInputStream());
                    scanner.useDelimiter("\\Z");
                    content = scanner.next();
                    scanner.close();
                }catch ( Exception ex ) {
                    ex.printStackTrace();
                    continue;
                }
                long end = System.currentTimeMillis();
                counter ++;
                if(counter == 65) { Thread.sleep(500); counter = 0;}


            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

public class SimulationNonReg {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        long init_time = System.currentTimeMillis();
        long start_timer = 0;
        long end_timer = 0;
        int  counter_per_sec = 0;
        long counter_sec = 0;

        BufferedReader br = null;
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Elena/Documents/Reply/7request_sec/responses", true)));
        PrintWriter result = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Elena/Documents/Reply/7request_sec/result", true)));

        Tomcat_threads[] pool_threads = new Tomcat_threads[0];
        for (int i=0; i<pool_threads.length; i++){
            pool_threads[i]= new Tomcat_threads();
            pool_threads[i].start();
        }
        int counter = 0;
        try {

            br = new BufferedReader(new FileReader("/Users/Elena/Documents/Reply/7request_sec/nonreg.csv.csv"));

            String line;
            long start;
            while( (line = br.readLine()) != null)
            {
                end_timer = System.currentTimeMillis() - init_time;

                //if( counter_tot == 2) break;
                String des_cosa = line.split(",")[1];
                String cd_visitatore = line.split(",")[0];

                String content = null;
                String url = "";
                URLConnection connection = null;
                try {
                    url = "http://54.154.233.250:8080/main.jsp?des_cosa=" ;
                    content = "ERRORE";
                    des_cosa = URLEncoder.encode(des_cosa, "UTF-8");
                    String suffix =  des_cosa + "&id=" + cd_visitatore;
                    //System.out.println(url);
                    //suffix = URLEncoder.encode(suffix, "UTF-8");
                    url = url  + suffix;
                    start = System.currentTimeMillis();
                    connection =  new URL(url).openConnection();
                    Scanner scanner = new Scanner(connection.getInputStream());
                    scanner.useDelimiter("\\Z");
                    content = scanner.next();
                    scanner.close();
                    counter_per_sec++;

                    if( (end_timer - start_timer)/1000 >= 1 ){
                        counter_sec = counter_per_sec * 1000/(end_timer - start_timer);
                        System.out.println(counter_per_sec);
                        System.out.println(counter_sec);
                        start_timer = System.currentTimeMillis() - init_time;
                        //System.out.println(start_timer);
                        //end_timer = 0;
                        counter_per_sec = 0;
                    }

                    System.out.println(content);
                }catch ( Exception ex ) {
                    ex.printStackTrace();
                    continue;
                }
                long end = System.currentTimeMillis();
                out.println((end-start));
                result.println((content));
                counter ++;
                if(counter == -1) { Thread.sleep(50); counter = 0;

                    out.close();
                    result.close();
                    out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Elena/Documents/Reply/7request_sec/responses", true)));
                    result = new PrintWriter(new BufferedWriter(new FileWriter("/Users/Elena/Documents/Reply/7request_sec/result", true)));

                }
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        out.close();



    }

}


