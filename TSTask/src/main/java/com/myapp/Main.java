package com.myapp;

import com.myapp.csv.FileHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * The main entry point of program
 */
public class Main {

    public static void main(String[]args) throws  IOException, InterruptedException{
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input the path in which the csv is located:");
        String inputFile = scanner.next();
        if(!FileHelper.isExistedPath(inputFile)) {
            System.out.println("The input file path is not existed. Please check it.");
            System.exit(1);
        }
        long totalLines = 0;
        System.out.println("Total lines including the header:");
        try {
            totalLines = Long.parseLong(scanner.next());
        } catch(NumberFormatException ex) {
            System.out.println("Invalid input total lines");
            System.exit(1);
        }
        if(totalLines <=1) {
            System.out.println("Total lines is must greater than 1 including the header");
            System.exit(1);
        }
        System.out.println("Finding real activation dates .... ");
        RealActivationDateFinder finder = new RealActivationDateFinder(totalLines, inputFile);
        Path resultOutPath = finder.execute();
        System.out.println("The result is located at path:" + resultOutPath.toString());
    }
}
