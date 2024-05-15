package org.hv.exercise.constant;

public enum FileTypeEnum {
    CSV("csv"),
    PARQUET("parquet");

    private final String fileType;

    FileTypeEnum(String fileType){
        this.fileType = fileType;
    }

    public String value() {
        return fileType;
    }
}
