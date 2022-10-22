package com.database.jdbc;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVReaderHeaderAware;

import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DruidDemo {
    static Properties prop;
    static DataSource dataSource;
    static Connection conn;

    static final String fileName1 = "jdbc-demo/data/room.csv";
    static final String fileName2 = "jdbc-demo/data/student.csv";
    static final String tableName1 = "room";
    static final String tableName2 = "student";

    public static void main(String[] args) throws Exception {
        // prepare environment until connection ready
        prepareAndConnect();
        System.out.println("Connect established. The connection is " + conn);
        runScript("jdbc-demo/script/createStudentTable.sql");
        /*System.out.println( "insert: " +
                insert(fileName1, tableName1) + "rows have been affected"
        );
        System.out.println( "insert: " +
                insert(fileName2, tableName2) + "rows have been affected"
        );*/
        conn.close();
    }

    private static void runScript(String scriptName) throws Exception {
        String allCommands = new String(Files.readAllBytes(Paths.get(scriptName)));
        String[] commands = allCommands.split(";");
        Statement stmt = conn.createStatement();
        for(String command : commands){
            stmt.execute(command);
        }
    }

    private static List<String[]> readAllFromCSV(String file) throws Exception {
        FileReader filereader = new FileReader(file);
        CSVReader csvReader = new CSVReader(filereader);
        List<String[]> allData = csvReader.readAll();
        for (String[] row : allData) {
            for (String cell : row) {
                System.out.print(cell + "\t");
            }
            System.out.println();
        }
        return allData;
    }

    private static int insert(String fileName, String tableName) throws Exception{
        int count = 0;
        List<String[]> allData = readAllFromCSV(fileName);
        List<String> allDataType = tableFieldTypes(tableName);
        int m = allData.size()-1;
        String s1 = getParameterSign(allData.get(0));
        String s2 = getValueSign(allData.get(0).length);
        for(int i=1; i<m; i++){
            String sql = String.format("insert into %s(%s) values (%s)", tableName, s1, s2);
            System.out.println(sql);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for(int j =0; j<allData.get(0).length; j++){
                if(noValue(allDataType.get(j))){
                    pstmt.setString(j+1, "null");
                } /*else if(needQuote(allDataType.get(j))){
                    pstmt.setString(j+1, "'"+allData.get(i)[j]+"'");
                } */else {
                    System.out.println(i);
                    System.out.println(j);
                    System.out.println(allData.get(i)[j]);
                    pstmt.setString(j+1, allData.get(i)[j]);
                }
                System.out.println(sql);
            }
            System.out.println(sql);
            count += pstmt.executeUpdate();
            pstmt.close();
        }
        return count;
    }

    private static boolean noValue(String s) {
        return s== null || s.length() == 0;
    }

    private static List<String> tableFieldTypes(String tableName) throws Exception {
        String sql = String.format("select * from %s", tableName);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery(sql);
        return getFieldTypes(rs);
    }

    private static String getParameterSign(String[] types) {
        int m = types.length;
        String ans = new String();
        for(int i=0;i<m-1;i++){
            ans = ans.concat(types[i] + ",");
        }
        ans = ans.concat(types[m-1]);
        return ans;
    }

    private static String getValueSign(int m) {
        String ans = new String();
        for(int i=0;i<m-1;i++){
            ans = ans.concat("?,");
        }
        ans = ans.concat("?");
        return ans;
    }

    private static boolean needQuote(String s) {
        boolean flag = false;
        String[] needTypes = {"CHAR", "VARCHAR","TEXT", "DATE", "DATETIME", "TIMESTAMP", "YEAR", "TIME"};
        for( String type : needTypes){
            if(s.equalsIgnoreCase(type)){
                flag = true;
                break;
            }
        }
        return flag;
    }

    private static void prepareAndConnect() throws Exception {
        prop = new Properties();
        prop.load(new FileReader("jdbc-demo/src/druid.properties"));
        dataSource = DruidDataSourceFactory.createDataSource(prop);
        conn = dataSource.getConnection();
    }

    private static List<String> getFields(ResultSet rs) throws Exception {
        List<String> fields = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        for(int i=1; i<=rsmd.getColumnCount();i++){
            fields.add(rsmd.getColumnName(i));
        }
        return fields;
    }

    private static List<String> getFieldTypes(ResultSet rs) throws Exception {
        List<String> fieldTypes = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        for(int i=1; i<=rsmd.getColumnCount();i++){
            fieldTypes.add(rsmd.getColumnTypeName(i));
        }
        return fieldTypes;
    }

}