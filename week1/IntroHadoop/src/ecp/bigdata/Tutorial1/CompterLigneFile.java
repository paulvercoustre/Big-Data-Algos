package ecp.bigdata.Tutorial1;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



//This function counts the number of lines in the input document and prints the column 6 & 7

public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("/Users/paulvercoustre/Documents/Education/Centrale_ESSEC/Big_Data_Algorithms_Plateforms/Week_1/Lab/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		int line_count = 0;
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			String annee = new String();
			String hauteur = new String();
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				
				// Process of the current line
				line_count += 1;
				annee = line.toString().split(";")[6];
				hauteur = line.toString().split(";")[7];
				
				System.out.println(annee + ";" + hauteur);
				
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}
		System.out.println("Number of lines " + line_count);
		
		
	}

}
