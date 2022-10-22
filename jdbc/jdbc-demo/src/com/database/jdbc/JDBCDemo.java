package com.database.jdbc;

import com.mysql.cj.protocol.Resultset;

import java.sql.*;

public class JDBCDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");

        final String URL = "jdbc:mysql://localhost:3306/lab1demo?useServerPrepStmts=true";
        final String USER = "root";
        final String PASSWORD = "123";
        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);

        String sql1 = "update room set papername = 'B' where kdname = 'kkk' ";
        String sql2 = "select * from room where papername = ?";

        Statement stmt = conn.createStatement();
        PreparedStatement pstmt = conn.prepareStatement(sql2);

        pstmt.setString(1, "D");

        // int count = stmt.executeUpdate(sql1);
        ResultSet rs = pstmt.executeQuery();

        // System.out.println("result is " + count);
        while(rs.next()){
            int kdno = rs.getInt("kdno");
            String kdname = rs.getString("kdname");
            Timestamp exptime = rs.getTimestamp("exptime");
            System.out.println("result is " + kdno);
            System.out.println("result is " + kdname);
            System.out.println("result is " + exptime);
        }


        rs.close();
        pstmt.close();
        conn.close();





    }



}
