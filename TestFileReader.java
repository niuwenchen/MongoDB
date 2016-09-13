package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TestFileReader {

	public static void main(String[] args) {
		File file = new File("/kafkadata");
		try {
			FileReader  reader= new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			
			StringBuffer sb = new StringBuffer();
			String line=br.readLine();
			while(line != null)
			{
				sb.append(line);
				System.out.println(line);
				line=br.readLine();
				
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
