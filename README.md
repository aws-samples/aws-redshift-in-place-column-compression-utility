## Project Description

Overview:

The utility leverages the Analyze and Alter Compression feature of Redshift to automate the compression of schema tables on the cluster. This helps customer achieve maximum reduction on the storage footprint as well as completely eliminates the manual effort leading to drastic reduced work hours of a dedicated human resource.
  
Key-Features:
  
  - Security: The utility leverages the Boto3 Redshift API to securely access the cluster and execute SQL commands. Secret Manger     id/friendly alias is being passed as a command line argument to retrieve credentials making it a completely secure utility.
  
  - Logging: On the cluster, The utility creates a control table if not exists already and successfully logs all the changes to       review the number of tables being analyzed only, compressed and table columns disk reduction for a schema.
  
  - Command line Arguments: The utility is completely dynamic and outputs based on user input:
    
    o Cluster identifier name
    
    o Database Name
    
    o Secret Id - Username and Password
    
    o Schema Name
    
    o Compression Mode: The command line argument passed as a user input enables
    them to custom configure for a mode of compression:
    
        Compress-all: The utility automatically analyze and compress required columns for all the tables in the provided schema.
       
        Compress-small: Based on provided threshold value, The utility automatically analyze and compress required columns for               tables < threshold value in TB for the provided schema
       
        Compress-large: Based on provided threshold value, The utility automatically analyze and compress required columns for               tables > threshold value in TB for the provided schema
    
    o Threshold Value: This is a user passed value in Terabytes that is required for Compress-small and Compress-large modes. Based     on the value and Compression mode, the utility will automatically analyze/compress tables less or greater than the threshold         value in size.

  - Exception Handling: The utility is fully equipped to handle errors/exceptions for generic as well as specific use-cases and have   been thoroughly tested.
           
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

