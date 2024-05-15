package org.hv.exercise.converter;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CSVToParquetConverter implements FileConverter {

    @Override
    public boolean convert(String csvFilePath, String parquetFilePath) {
        CSVReader csvReader = null;
        ParquetWriter<Group> writer = null;
        try {
            csvReader = new CSVReader(new FileReader(csvFilePath));
            String[] headers = csvReader.readNext();  // Read the header line

            //Create schema
            MessageType schema = constructSchema(headers);

            // Configuration for the Parquet writer
            Configuration conf = new Configuration();

            // Create ParquetWriter
            Path path = new Path(parquetFilePath);

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            writer = ExampleParquetWriter.builder(path)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withType(schema)
                    .withConf(conf)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withValidation(false)
                    .build();


            // Read each line from CSV and convert to Parquet Group
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                Group group = groupFactory.newGroup();
                for (int i = 0; i < headers.length; i++) {
                    group.append(headers[i], line[i]);
                }
                writer.write(group);
            }
        } catch (Exception any) {
            System.out.printf("Something wrong: %s%n", any.getMessage());
            return false;
        } finally {
            try {

                if (csvReader != null) {
                    csvReader.close();
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception ignore) {
                System.out.printf("Warning! %s%n", ignore.getMessage());
            }
        }
        removeCRCFile(parquetFilePath);
        return true;
    }

    private MessageType constructSchema(String[] headers) {
        // Create Parquet schema from CSV header
        List<Type> fields = new ArrayList<>();
        for (String header : headers) {
            fields.add(Types
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named(header)
            );
        }
        return new MessageType("csv_schema", fields);
    }

    private void removeCRCFile(String path) {
        try {

            java.nio.file.Path crc = Paths.get(path + ".crc");
            String directory = crc.getParent().toString();
            String filename = crc.getFileName().toString();

            // Concatenate the dot and the filename
            String newFilename = "." + filename;

            // Create a new Path object with the modified filename
            java.nio.file.Path newPath = Paths.get(directory, newFilename);

            Files.deleteIfExists(newPath);
        } catch (Exception e) {
            System.out.println("Cannot remove: crc file. Ignore!");
        }
    }
}
