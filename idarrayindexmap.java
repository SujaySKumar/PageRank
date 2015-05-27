import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;


final class idarrayindexmap {
	
	public static Map<Integer,Integer> readSqlFile(File file) throws IOException {
		long startTime = System.currentTimeMillis();
		Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		
		SqlReader in = new SqlReader(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8")), "page");
		try {
			while (true) {
				List<List<Object>> data = in.readInsertionTuples();
				if (data == null)
					break;
				int i=0;
				for (List<Object> tuple : data) {
					//if (tuple.size() != 12)
						//throw new IllegalArgumentException();
					
					Object namespace = tuple.get(1);
					if (!(namespace instanceof Integer))
						throw new IllegalArgumentException();
					if (((Integer)namespace).intValue() != 0)
						continue;
					
					Object id = tuple.get(0);
					Object title = tuple.get(2);
					if (!(id instanceof Integer && title instanceof String))
						throw new IllegalArgumentException();
					if (result.containsKey(title))
						throw new IllegalArgumentException();
					result.put((Integer)id, (Integer)i);
					i=i+1;
				}
				System.out.printf("\rParsing %s: %.3f million entries stored...", file.getName(), result.size() / 1000000.0);
			}
		} finally {
			in.close();
		}
		
		System.out.printf("\rParsing %s: %.3f million entries stored... Done (%.2f s)%n", file.getName(), result.size() / 1000000.0, (System.currentTimeMillis() - startTime) / 1000.0);
		return result;
	}
	
	
	
	
	public static <K,V> Map<V,K> reverseMap(Map<K,V> map) {
		Map<V,K> result = new HashMap<V,K>();
		for (K key : map.keySet())
			result.put(map.get(key), key);
		return result;
	}
	
	
	
	private idarrayindexmap() {}  // Not instantiable
	
}
