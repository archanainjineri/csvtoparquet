package parquet.compat.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class ConvertUtils {

	private static final Log LOG = Log.getLog(ConvertUtils.class);

	public static final String CSV_DELIMITER = "|";

	private static String readFile(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		StringBuilder stringBuilder = new StringBuilder();

		try {
			String line = null;
			String ls = System.getProperty("line.separator");

			while ((line = reader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				LOG.warn("Exception closing reader " + reader + ": " + ioe.getMessage());
			}
		}

		return stringBuilder.toString();
	}

	public static String getSchema(File csvFile) throws IOException {
		String fileName = csvFile.getName().substring(0, csvFile.getName().length() - ".csv".length()) + ".schema";
		File schemaFile = new File(csvFile.getParentFile(), fileName);
		return readFile(schemaFile.getAbsolutePath());
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
		convertCsvToParquet(csvFile, outputParquetFile, false);
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary)
			throws IOException {
		LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
		String rawSchema = getSchema(csvFile);
		if (outputParquetFile.exists()) {
			throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + " already exists");
		}

		Path path = new Path(outputParquetFile.toURI());

		MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
		CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line;
		int lineNumber = 0;
		try {
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
				writer.write(Arrays.asList(fields));
				++lineNumber;
			}

			writer.close();
		} finally {
			LOG.info("Number of lines: " + lineNumber);
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException ioe) {
				LOG.warn("Exception closing reader " + br + ": " + ioe.getMessage());
			}
		}
	}

	public static File[] getAllOriginalCSVFiles() {
		File baseDir = new File("./src/test/resources");
		final File[] csvFiles = baseDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});
		return csvFiles;
	}

	public static File getParquetOutputFile(String name, String module, boolean deleteIfExists) {
		File outputFile = new File("./target/parquet/", getParquetFileName(name, module));
		outputFile.getParentFile().mkdirs();
		if (deleteIfExists) {
			outputFile.delete();
		}
		return outputFile;
	}

	private static String getParquetFileName(String name, String module) {
		return name + (module != null ? "." + module : "") + ".parquet";
	}

	public static String getFileNamePrefix(File file) {
		return file.getName().substring(0, file.getName().indexOf("."));
	}

	public static void main(String[] args) throws IOException {

		File[] csvFiles = getAllOriginalCSVFiles();
		if(csvFiles.length == 0)
		{
			LOG.error("No CSV files found in the location given");
		}
			
		for (File csvFile : csvFiles) {
			String filename = getFileNamePrefix(csvFile);
			File parquetTestFile = getParquetOutputFile(filename, "test", true);
			LOG.info(parquetTestFile.getAbsolutePath());
			ConvertUtils.convertCsvToParquet(csvFile, parquetTestFile);
			LOG.info("Parquet file has been saved at "+ parquetTestFile.getAbsolutePath());
		}

	}

}
