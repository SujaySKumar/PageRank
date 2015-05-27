import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class tempmain{

	private static final File PAGE_ID_TITLE_SQL_FILE = new File("simplewiki-latest-page.sql.gz"); 
	private static final File PAGE_LINKS_SQL_FILE = new File("simplewiki-latest-pagelinks.sql.gz");
	private static Map<Integer,Integer> arrayindex;
	private static Map<String,Integer> idByTitle;
	
	public static void main(String args[]) throws IOException{
		

		double a[][]=new double[7922][7922];
		int outdegree[]=new int[7922];

		/*Map<Integer,Integer>*/ arrayindex = idarrayindexmap.readSqlFile(PAGE_ID_TITLE_SQL_FILE);
		int max=0;

		/*for (Map.Entry<Integer, Integer> entry : arrayindex.entrySet()) {
			if(entry.getValue()>max)max=entry.getValue();
			System.out.println("Max="+max);

    		//System.out.println(entry.getKey()+" : "+entry.getValue());
		}*/
	

	idByTitle = PageIdTitleMap.readSqlFile(PAGE_ID_TITLE_SQL_FILE);
	/*for (Map.Entry<String, Integer> entry : idByTitle.entrySet()) {
    		System.out.println(entry.getKey()+" : "+entry.getValue());
		}*/


	SqlReader in = new SqlReader(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(PAGE_LINKS_SQL_FILE)), "UTF-8")), "pagelinks");
		try {
			while (true) {
				List<List<Object>> data = in.readInsertionTuples();
				if (data == null)
					break;
				
				for (List<Object> tuple : data) {
					//if (tuple.size() != 3)
						//throw new IllegalArgumentException();
					
					Object srcId = tuple.get(0);
					Object namespace = tuple.get(1);
					Object destTitle = tuple.get(2);
					if (!(srcId instanceof Integer && namespace instanceof Integer && destTitle instanceof String))
						throw new IllegalArgumentException();

					int source = (Integer)srcId;
					//System.out.println(destTitle);
					if(!(idByTitle.containsKey(destTitle)))continue;
					int dest = (Integer)idByTitle.get(destTitle);

					if(!(arrayindex.containsKey(source)))continue;
					if(!(arrayindex.containsKey(dest)))continue;

					int i=arrayindex.get(source);
					int j=arrayindex.get(dest);

					//if(i>=7921 || j>=7921)System.out.println("i="+i+" j="+j);
					a[j][i]++;
					outdegree[i]++;



				}

			}
		}
		finally{
			in.close();
		}
		for(int i=0;i<7922;i++){
			if(outdegree[i]!=0){
				for(int j=0;j<7922;j++){
					a[j][i]=0.85*a[j][i]/outdegree[i];
				}
			}
			else {
				for(int j=0;j<7922;j++){
					a[j][i]=0.85/7922;
				}
			}
		}
		normalPageRank(a,0.85,7922,100);
		/*for(int i=0;i<7921;i++){
			for(int j=0;j<7921;j++)System.out.print(a[i][j]+" ");
			System.out.println();
		}*/
	}


	public static void normalPageRank(double [][]a, double damp, int N, int clicks) throws IOException
 	{
	 for(int i=0;i<N;i++)
		 for(int j=0;j<N;j++)
			 a[i][j]+= (1-damp)/N;
	 double[] rank = new double[N]; 
	 
	 //Initializing rank equally to all pages at the beginning.
	 
	 for(int i=0;i<N;i++)
		 rank[i]=1/(double)N;
	 
     for (int t = 0; t < clicks; t++) {

         double[] newRank = new double[N]; 
         for (int j = 0; j < N; j++) {
             for (int k = 0; k < N; k++) 
                newRank[j] += rank[k] * a[j][k]; 
         } 

         // Update page ranks.
         rank = newRank;
     }
     
     System.out.println("Wiki PageRank is ");
     display(rank,N);
 	}

 	public static void display(double a[],int N) throws IOException
 {
 	//Map<Integer,Integer> revMap = idarrayindexmap.reverseMap(arrayindex);
 	Map<Integer,String> titleById = PageIdTitleMap.reverseMap(idByTitle);

 	TreeMap<Double,Integer> treemap = new TreeMap();
 	double sum=0;
	 for(int i=0;i<N;i++)
	 {
	 	treemap.put(a[i],i);
         //System.out.printf("%7.4f \n", Math.log10(a[i])); 
         sum=sum+a[i];
	 }
	 Map<Double,Integer> desc=treemap.descendingMap();
	 // Get a set of the entries
      Set set = desc.entrySet();
      // Get an iterator
      Iterator i = set.iterator();
      // Display elements
      int count=0;
      while(i.hasNext() ) {
         Map.Entry me = (Map.Entry)i.next();
         //System.out.print(me.getKey() + ": ");
         int pageid=(Integer)me.getValue();
         String pagename=titleById.get(pageid);
         if(pagename!=null){
         	double pr=(Double)me.getKey();
         	System.out.print(pr + ": ");
         System.out.println("Page="+pagename);
     }

         //System.out.println(revMap.get(pageid));
         count++;
      }
      //System.out.println("h1y");
	 //System.out.println("Sum="+sum);
	 
 }
}