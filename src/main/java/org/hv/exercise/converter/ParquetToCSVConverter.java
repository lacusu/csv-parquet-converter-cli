package org.hv.exercise.converter;

import com.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ParquetToCSVConverter implements FileConverter {

    @Override
    public boolean convert(String parquetFilePath, String csvFilePath) {
        Configuration conf = new Configuration();
        Path parquetPath = new Path(parquetFilePath);

        // Open the Parquet file reader
        ParquetFileReader reader = null;
        CSVWriter csvWriter = null;

        try {
            // Create an InputFile instance from the Path
            HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetPath, conf);
            reader = ParquetFileReader.open(inputFile);
            // Read Parquet file footer to get schema
            ParquetMetadata metadata = reader.getFooter();
            MessageType schema = metadata.getFileMetaData().getSchema();

            // Prepare to write CSV file
            csvWriter = new CSVWriter(new FileWriter(csvFilePath));

            // Write CSV header
            List<String> headers = new ArrayList<>();
            for (Type field : schema.getFields()) {
                headers.add(field.getName());
            }
            csvWriter.writeNext(headers.toArray(new String[0]));

            // Read Parquet file and write to CSV
            ColumnIOFactory factory = new ColumnIOFactory();
            MessageColumnIO columnIO = factory.getColumnIO(schema);

            // Iterate over row groups
            for (BlockMetaData block : metadata.getBlocks()) {
                // Read next row group
                var pages = reader.readNextRowGroup();
                if (pages == null) break; // No more row groups

                long rows = pages.getRowCount();
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (long i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    List<String> row = new ArrayList<>();
                    for (int j = 0; j < group.getType().getFieldCount(); j++) {
                        row.add(group.getValueToString(j, 0));
                    }
                    csvWriter.writeNext(row.toArray(new String[0]));
                }
            }
        } catch (Exception any) {
            System.out.printf("Something wrong: %s%n", any.getMessage());
            return false;
        } finally {
            try {
                if (csvWriter != null) {
                    csvWriter.close();
                }
                if (reader != null) {

                    reader.close();
                }
            } catch (Exception ignore) {
                System.out.printf("Warning! %s%n", ignore.getMessage());
            }
        }
        return true;
    }
}
