package org.hv.exercise.converter;

import org.hv.exercise.constant.FileTypeEnum;

import static org.hv.exercise.constant.Message.CONVERTING_MSG;
import static org.hv.exercise.constant.Message.INVALID_FILE_PATH_MSG;
import static org.hv.exercise.constant.Message.UNSUPPORTED_CONVERSION_MSG;

public class FileConverterContext {
    private FileConverter converter;

    public void setConverter(FileConverter converter) {
        this.converter = converter;
    }

    public boolean convertFile(String inputFilePath, String outputFilePath) throws Exception {

        // Get file extensions
        String inputFileExt = getFileExtension(inputFilePath);
        String outputFileExt = getFileExtension(outputFilePath);

        // Determine converter based on file extensions
        if (inputFileExt.equalsIgnoreCase(FileTypeEnum.CSV.value()) && outputFileExt.equalsIgnoreCase(FileTypeEnum.PARQUET.value())) {
            setConverter(new CSVToParquetConverter());
        } else if (inputFileExt.equalsIgnoreCase(FileTypeEnum.PARQUET.value()) && outputFileExt.equalsIgnoreCase(FileTypeEnum.CSV.value())) {
            setConverter(new ParquetToCSVConverter());
        } else {
            throw new IllegalArgumentException(String.format(UNSUPPORTED_CONVERSION_MSG, inputFileExt, outputFileExt));
        }
        System.out.printf(CONVERTING_MSG, inputFileExt, outputFileExt);

        // Perform the conversion
        return converter.convert(inputFilePath, outputFilePath);
    }

    private String getFileExtension(String filePath) {
        int lastIndex = filePath.lastIndexOf('.');
        if (lastIndex <= 0) {
            throw new IllegalArgumentException(String.format(INVALID_FILE_PATH_MSG, filePath));
        }
        return filePath.substring(lastIndex + 1).toLowerCase();
    }
}
