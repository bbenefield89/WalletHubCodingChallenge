package com.ef;

import java.sql.*;

import io.github.cdimascio.dotenv.Dotenv;

public class Parser {
    private Connection dbConnection;
    private String pathToAccessLogFile;
    private String startDate;
    private String duration;
    private String threshold;
    private String loadAccessLogFileToDbQuery;
    private String usersOverAPILimitQuery;

    public static void main(String args[]) throws Exception {
        try {
            Dotenv dotenv = Dotenv.load();
            String dbURL = dotenv.get("DB_URL");
            String dbUsername = dotenv.get("DB_USERNAME");
            String dbPassword = dotenv.get("DB_PASSWORD");
            Parser parser = new Parser(dbURL, dbUsername, dbPassword, args);

            parser.loadAccessLogFileToDB();
            parser.setUsersOverAPILimitQuery();
            parser.executeParserQuery();
        }
        catch (Exception e) {
            throw new Exception(e);
        }
    }

    Parser(String dbURL, String dbUsername, String dbPassword, String args[]) throws SQLException {
        try {
            this.dbConnection = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
            this.parseCLIArgs(args);
            this.setLoadAccessLogFileToDbQuery();
        }
        catch(SQLException e) {
          throw new SQLException(e);
        }
    }

    public void loadAccessLogFileToDB() throws SQLException {
        try {
          PreparedStatement loadAccessLogFileToDB = this.insertParamValsInLoadAccessLogFileToDBQuery();
          loadAccessLogFileToDB.execute();
        }
        catch(SQLException e) {
          throw new SQLException(e);
        }
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

    private PreparedStatement insertParamValsInLoadAccessLogFileToDBQuery() throws SQLException {
        PreparedStatement preparedStatement = null;

        try {
          preparedStatement = this.dbConnection.prepareStatement(this.loadAccessLogFileToDbQuery);
          preparedStatement.setString(1, this.pathToAccessLogFile);
        }
        catch (SQLException e) {
          throw new SQLException(e);
        }

        return preparedStatement;
    }
    
    private void setLoadAccessLogFileToDbQuery() {
        this.loadAccessLogFileToDbQuery = "" +
            "load data local infile ? into table log_activity " +
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

    private void parseCLIArgs(String args[]) {
        this.parsePathToAccessLogFile(args[0]);
        this.parseStartDate(args[1]);
        this.parseDuration(args[2]);
        this.parseThreshold(args[3]);
    }

    private void parsePathToAccessLogFile(String pathToAccessLogFile) {
        pathToAccessLogFile = this.getCLIArgValue(pathToAccessLogFile);
        this.setPathToAccessLogFile(pathToAccessLogFile);
    }

    private String getPathToAccessLogFile() {
        return this.pathToAccessLogFile;
    }

    private void setPathToAccessLogFile(String pathToAccessLogFile) {
        this.pathToAccessLogFile = pathToAccessLogFile;
    }

    private void parseStartDate(String startDate) {
        startDate = this.getCLIArgValue(startDate).replace(".", " ");
        this.setStartDate(startDate);
    }


    private void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    private void parseDuration(String duration) {
        duration = this.getCLIArgValue(duration);
        this.setDuration(duration);
    }

    private void setDuration(String duration) {
        this.duration = duration.equals("hourly") ? "hour" : duration.equals("daily") ? "day" : null;;
    }

    private void parseThreshold(String threshold) {
        threshold = this.getCLIArgValue(threshold);
        this.setThreshold(threshold);
    }

    private void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    private String getCLIArgValue(String cliArg) {
        return cliArg.split("=", 2)[1];
    }

    private void printAllUsersOverAPILimit() throws Exception {
        try {
            PreparedStatement preparedStatement = this.insertParamValsInUsersOverApiLimitQuery();
            ResultSet usersOverAPILimitQueryResults  = preparedStatement.executeQuery();

            while (usersOverAPILimitQueryResults.next()) {
                String ip_address = usersOverAPILimitQueryResults.getString("ip_address");
                System.out.println(ip_address);
            }
        }
        catch(SQLException e) {
          throw new SQLException(e);
        }
        catch (Exception e) {
          throw new Exception(e);
        }
    }

    private PreparedStatement insertParamValsInUsersOverApiLimitQuery() throws SQLException {
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = dbConnection.prepareStatement(this.usersOverAPILimitQuery);
            preparedStatement.setString(1, this.startDate);
            preparedStatement.setString(2, this.startDate);
            preparedStatement.setInt(3, Integer.parseInt(this.threshold));
        }
        catch (SQLException e) {
          throw new SQLException(e);
        }

        return preparedStatement;
    }
}
