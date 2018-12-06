package com.ef;

import java.sql.*;

public class Parser {
    public static void main(String args[]) throws Exception {
        try {
            Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/wallethub_parser",
                "root",
                "root"
            );

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from log_activity limit 1;");

            while (rs.next()) {
                int id = rs.getInt("id");
                String ip_address = rs.getString("ip_address");
                String request_type = rs.getString("request_type");
                int http_status_code = rs.getInt("http_status_code");
                String useragent = rs.getString("useragent");

                System.out.printf("\n%d %s %s %d %s\n\n",
                    id, ip_address, request_type, http_status_code, useragent
                );
            }

            con.close();
        }
        catch (SQLException e) {
            System.out.println(e);
        }
    }
}
