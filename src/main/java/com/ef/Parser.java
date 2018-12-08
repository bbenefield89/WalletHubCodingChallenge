package com.ef;

import java.sql.*;

public class Parser {
    private Connection dbConnection;
    private String startDate;
    private String duration;
    private String threshold;
    private String loadAccessLogFileToDbQuery;
    private String usersOverAPILimitQuery;

    public static void main(String args[]) throws Exception {
        try {
            String dbURL = "jdbc:mysql://localhost:3306/wallethub_parser";
            String dbUsername = "root";
            String dbPassword = "root";
            Parser parser = new Parser(dbURL, dbUsername, dbPassword);

            parser.loadAccessLogFileToDB();
            parser.parseCLIArgs(args);
            parser.setUsersOverAPILimitQuery();
            parser.executeParserQuery();
        }
        catch (Exception e) {
            System.out.println("main method");
            throw new Exception(e);
        }
    }

    Parser(String dbURL, String dbUsername, String dbPassword) throws Exception {
        try {
            this.dbConnection = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
            this.setLoadAccessLogFileToDbQuery();
        }
        catch (Exception e) {
            System.out.println("Constructor method");
            throw new Exception(e);
        }
    }

    public void loadAccessLogFileToDB() throws Exception {
        try {
            Statement statement = this.dbConnection.createStatement();
            statement.executeQuery(this.getLoadAccessLogFileToDbQuery());
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    public String getLoadAccessLogFileToDbQuery() {
        return this.loadAccessLogFileToDbQuery;
    }

    public void setLoadAccessLogFileToDbQuery() {
        this.loadAccessLogFileToDbQuery = "" +
            "load data local infile './access.log' into table log_activity " +
            "fields terminated by '|' " +
            "lines terminated by '\\n' " +
            "(@c1, @c2, @c3, @c4, @c5) " +
            "set " +
            "  created_at=@c1, " +
            "  ip_address=@c2, " +
            "  request_type=@c3, " +
            "  http_status_code=@c4, " +
            "  useragent=@c5;";
    }

    public void parseCLIArgs(String args[]) {
        this.parseStartDate(args[0]);
        this.parseDuration(args[1]);
        this.parseThreshold(args[2]);
    }

    public void parseStartDate(String startDate) {
        startDate = this.splitCLIArgumentAtEqualSign(startDate).replace(".", " ");
        this.setStartDate(startDate);
    }


    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public void parseDuration(String duration) {
        duration = this.splitCLIArgumentAtEqualSign(duration);
        this.setDuration(duration);
    }

    public void setDuration(String duration) {
        this.duration = duration.equals("hourly") ? "hour" : duration.equals("daily") ? "day" : null;;
    }

    public void parseThreshold(String threshold) {
        threshold = this.splitCLIArgumentAtEqualSign(threshold);
        this.setThreshold(threshold);
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public String splitCLIArgumentAtEqualSign(String cliArg) {
        return cliArg.split("=", 2)[1];
    }

    public String getUsersOverAPILimitQuery() {
        return this.usersOverAPILimitQuery;
    }

    public void setUsersOverAPILimitQuery() {
        this.usersOverAPILimitQuery = "" +
            "select ip_address " +
            "from log_activity " +
            "where created_at " +
            "  between ?" +
            "  and date_add(?, interval 1 " + this.duration + ") " +
            "group by ip_address " +
            "having count(*) > ?;";
    }

    public void executeParserQuery() throws Exception {
        try {
            this.printAllUsersOverAPILimit();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    public void printAllUsersOverAPILimit() throws Exception {
        try {
            PreparedStatement preparedStatement = this.insertParamValuesInPreparedStatementQuery();
            ResultSet usersOverAPILimitQueryResults  = preparedStatement.executeQuery();

            while (usersOverAPILimitQueryResults.next()) {
                String ip_address = usersOverAPILimitQueryResults.getString("ip_address");
                System.out.println(ip_address);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    public PreparedStatement insertParamValuesInPreparedStatementQuery() throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = dbConnection.prepareStatement(this.usersOverAPILimitQuery);
            preparedStatement.setString(1, this.startDate);
            preparedStatement.setString(2, this.startDate);
            preparedStatement.setInt(3, Integer.parseInt(this.threshold));
        }
        catch (Exception e) {
            System.out.println("insertParamValuesInPreparedStatementQuery");
            System.out.println(e);
        }

        return preparedStatement;
    }
}
