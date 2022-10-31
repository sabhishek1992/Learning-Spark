import org.apache.spark.sql.api.java.UDF2;

public class ExtractFirstNDigits implements UDF2<String,Integer,String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String value, Integer noOfDigitsToExtract) throws Exception {
		if(value == null || value.isEmpty()) return value;
		int valueLength = value.length();// 15
		String extractValue = value.substring(0,noOfDigitsToExtract);// 0,10
		String laterPart = hideLaterPartOfString(valueLength-noOfDigitsToExtract);
		return extractValue+laterPart;
	}

	private String hideLaterPartOfString(Integer laterPartLength) {
		char[] valueArray = new char[laterPartLength];
		
		for (int i=0 ; i < laterPartLength ; i++) {
			valueArray[i] = 'X';
		}
		
		return String.valueOf(valueArray);
	}

}
