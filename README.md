# CSV Parquet Converter CLI

This CLI tool facilitates the conversion of data between CSV and Parquet formats. It accepts command-line options for specifying the input and output file paths, enabling users to convert data from CSV to Parquet and vice versa.

## How to Run

To run the CSV Parquet Converter CLI tool, follow these steps:

Checkout Repository: Clone or download the repository containing the CLI tool to your local machine.

Navigate to Repository Root: Open a terminal or command prompt and navigate to the root directory of the repository where the CLI tool is located.

### Package the source code

```
mvn clean package
```

### Run command

   ```
   java -jar ./target/converter.jar <options>
   ```

 Available options:

    -h,--help: Show help
    -i,--input <arg>: Input file path. File extension (.csv|.parquet) is expected
    -o,--output <arg>: Output file path. File extension (.csv|.parquet) is expected

### Supportability

#### 1. Convert CSV to parquet

  ```
  java -jar ./target/converter.jar -i ~/Downloads/22M.csv -o ~/Documents/out/22M.parquet
  ```

  Result:
  
  ![image](https://github.com/lacusu/csv-parquet-converter-cli/assets/7995583/3569797c-f498-4a95-92a6-e4e043275900)

  File Viewer (CSV on the left-hand side and PARQUET on the right-hand side)
  ![image](https://github.com/lacusu/csv-parquet-converter-cli/assets/7995583/b6aca883-3133-450f-b200-bec42d9405f9)


#### 2. Convert parquet to CSV

  ```
  java -jar ./target/converter.jar -i ~/Downloads/Flights1m.parquet -o ~/Documents/out/Flights1m.csv
  ```

  Result:
  
  ![image](https://github.com/lacusu/csv-parquet-converter-cli/assets/7995583/74a74fef-4bec-470f-97c8-a58f922be595)


  File Viewer (PARQUET on the left-hand side and CSV on the right-hand side)
  
  ![image](https://github.com/lacusu/csv-parquet-converter-cli/assets/7995583/bc69b005-66bb-4301-bc0d-63e21b5d9873)

## Contextual Converter Selection
Upon receiving input and output options, our application's file context module dynamically determines the appropriate file converter to use. This decision is based on the file extensions provided by the user, ensuring that the conversion process seamlessly adapts to the desired transformation.

## Libraries Used
opencsv: Easy-to-use CSV library for reading and writing CSV files.
parquet-avro: Provides functionality for reading and writing Parquet files.
hadoop-common: Essential components and utilities for the Hadoop ecosystem, such as file system abstractions.
hadoop-mapreduce-client-core: Components and utilities for developing MapReduce jobs within Hadoop-based applications.
commons-cli: Provides a simple API for parsing command line arguments passed to Java applications.



    
