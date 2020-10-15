package com.cba.assessment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecutorMain {
	
	
	 private String delimiter = "~";

	 public static void main(String[] args) {
		 ExecutorMain executor = new ExecutorMain();
		 System.out.println("Processing started successfully");
		 String inputFileName = "src/data/SampleOutput.txt";
		 String outputDirectory = "src/output/";
		 executor.cleanOutputDirectory(outputDirectory);
	     String fileContent = executor.getFileContent(inputFileName);
	     if(fileContent == null || fileContent.isEmpty()) {
	    	 System.out.println("Input file is empty");
	     }
	     String transactionDelimeter = "~smh~\\[record";
	     String[] transactions = fileContent.split(transactionDelimeter);
	     if(transactions != null) {
	    	 int transactionCount = 1;
		     try {
				for (String transaction : transactions) {
					String transactionData = executor.formatData(transaction).trim();
					if(transactionData != null && !transactionData.isEmpty() ) {
						String outputFileName = "";
						outputFileName = outputDirectory+"transaction_"+transactionCount+".csv";
						Files.write(Paths.get(outputFileName), transactionData.getBytes());
						transactionCount++;
					}
				 }
			} catch (IOException e) {
				e.printStackTrace();
			}
	     }
	     System.out.println("Processing completed successfully");
	 }
	 
	 public void cleanOutputDirectory(String path) {
		 File outputDirectory = new File(path); 
		   for(File f: outputDirectory.listFiles()) 
			   f.delete(); 
	 }

    public String getFileContent(String inputFileName) {
    	 String fileContent = "";
    	 try (Stream<String> stream = Files.lines(Paths.get(inputFileName))) {
		        fileContent = stream.map(s -> s.split("\\s+"))
		                              .map(s -> Arrays.stream(s).collect(Collectors.joining(delimiter))+"\n")
		                              .collect(Collectors.joining());
    	 }catch (IOException e) {
		        e.printStackTrace();
		    }
		return fileContent;
    }
	 
	 
	 public String formatData(String transactionContent) {
	 	String formattedString = "";
	    try {
	    		String[] streamArray = transactionContent.split( "\\n" );
	    		StringBuilder sb = new StringBuilder();
	    		for (String data : streamArray) {
	    			if(data != null && !data.isEmpty() && !data.contains("[record")){
	    				String key = "";
	    				String value = "";
	    				key = data.split(delimiter)[1];
	    				value = getValue(data);
	    				sb.append(key+","+value+ "\n");
	    			}
	    			formattedString = sb.toString();
				}
	        }catch(Exception e) {
		        e.printStackTrace();
		    }
	    return formattedString;
	 }
	 
	 public String getValue(String data) {
		 String value = "";
		 Pattern p = Pattern.compile("\"([^\"]*)\"");
			Matcher m = p.matcher(data);
			while (m.find()) {
				value = m.group(1);
				if(value.trim().equals(delimiter)) {
					value = " ";
				}
			}
			return value;
	 }
}