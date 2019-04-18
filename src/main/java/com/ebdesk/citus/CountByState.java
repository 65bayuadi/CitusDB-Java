package com.ebdesk.citus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CountByState {

    public static void main(String[] args) throws IOException {
        Set<String> state = new HashSet<String>();
        BufferedReader br = new BufferedReader(new FileReader("assets/city_lookup.csv"));
        while (br.readLine() != null) {
            String line = br.readLine();
            state.add(line.split(",")[1]);
        }


        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://192.168.24.120:9700/postgres",
                            "postgres", "");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            ResultSet rs = null;

            for (String s : state) {
                System.out.println(" ================================= " + s + " ================================= ");
                Long startTime = System.currentTimeMillis();
                System.out.println("started on = " + startTime.toString());
                stmt = c.createStatement();
                rs = stmt.executeQuery("select count(*) from fb_profile_new  where state = '" + s + "';");
                while (rs.next()) {
                    String data = rs.getString(1);

                    System.out.println(data);
                }
                Long endTime = System.currentTimeMillis();
                System.out.println("finished on = " + endTime);
                System.out.println("Time = " + getDateDiff(startTime, endTime, TimeUnit.MILLISECONDS));
                System.out.println(" ============================================================================= ");
            }


            rs.close();
            stmt.close();
            c.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Operation done successfully");

    }

    public static long getDateDiff(long timeUpdate, long timeNow, TimeUnit timeUnit) {
        long diffInMillies = Math.abs(timeNow - timeUpdate);
        return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
    }
}
