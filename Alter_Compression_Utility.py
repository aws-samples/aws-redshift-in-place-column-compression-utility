 # Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 # SPDX-License-Identifier: MIT-0
 #
 # Permission is hereby granted, free of charge, to any person obtaining a copy of this
 # software and associated documentation files (the "Software"), to deal in the Software
 # without restriction, including without limitation the rights to use, copy, modify,
 # merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 # permit persons to whom the Software is furnished to do so.
 #
 # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 # INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 # PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 # HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 # OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 # SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import boto3
import sys
import json
import time
import botocore
from tqdm import tqdm

def execute_query(clusterid, db, secret, query):
    try:
        client = boto3.client('redshift-data')
        response = client.execute_statement(
            ClusterIdentifier=clusterid,
            Database=db,
            SecretArn=secret,
            Sql=query
        )

        while True:
            response=client.describe_statement(Id=response['Id'])
            if (response["Status"] == "FINISHED" or response["Status"] == "ABORTED" or response["Status"] == "FAILED"):
                #print("Query Status: {0}".format(response["Status"]))
                if(response["Status"] == "FAILED"):
                    print("\nEXECUTION ERROR {0}".format(response["Error"]))
                    raise Exception
                elif(response["Status"] == "ABORTED"):
                    print("\n\tQuery has been aborted\n")
                    raise Exception
                #else:
                #    print("EXECUTION ERROR: None")
                break

            else:
                #print("Query Status: {0}".format(response["Status"]))
                time.sleep(2)

        if (response["HasResultSet"] == True):
            result=client.get_statement_result(
                Id=response['Id']
            )
            return result

    except client.exceptions.ValidationException as Ve:
        sys.exit("Validation Exception: \n\n\t{0}".format(Ve))

    except client.exceptions.ExecuteStatementException as ESe:
        sys.exit("\nExecute Statement Exception: \n\n\t{0}\n".format(ESe))
    
    except client.exceptions.ActiveStatementsExceededException as ASEe:
        sys.exit("\nActive Statements Exceeded Exception: \n\n\t{0}\n".format(ASEe))
    
    except client.exceptions.ResourceNotFoundException as RNFe:
        sys.exit("\n Resource not Found Exception: \n\n\t{0}\n".format(RNFe))

    except client.exceptions.InternalServerException as ISe:
        sys.exit("\nInternal Server Exception: \n\n\t{0}\n".format(ISe))
    
    except client.exceptions.ClientError as CEe:
        sys.exit("\nClient Error Exception: \n\n\t{0}\n".format(CEe))

    except:
        sys.exit("{0}".format(sys.exc_info()[1]))

def create_control_table(clusterid, db, secret):

    createQuery = "CREATE TABLE IF NOT EXISTS public.encoding_control \
    (  \
	schema_name character varying(100) NOT NULL ENCODE lzo, \
	table_name character varying(100) NOT NULL ENCODE lzo,   \
	column_name character varying(100) NOT NULL ENCODE lzo,  \
	curr_encoding_type character varying(20) NOT NULL ENCODE lzo, \
    recmd_encoding_type character varying(20) NOT NULL DEFAULT 'unknown' ENCODE lzo, \
    est_red_pct character varying(20) NOT NULL DEFAULT '0.00' ENCODE lzo, \
    analyze_ind character(1) NOT NULL DEFAULT 'N'::bpchar ENCODE lzo, \
	alter_status character(1) NOT NULL DEFAULT 'N'::bpchar ENCODE lzo,   \
	update_timestamp timestamp without time zone ENCODE az64 \
    )  \
    DISTSTYLE AUTO;"

    for i in tqdm (range (1), desc="Creating Control Table if not exists.."):
        execute_query(clusterid, db, secret, createQuery)

def truncate_control_table(clusterid, db, secret):

    truncateQuery = "truncate public.encoding_control"

    for i in tqdm (range (1), desc="Truncating Control Table:.."):
        execute_query(clusterid, db, secret, truncateQuery)

def load_control_table(clusterid, db, secret, schemaName):

    listTables = "select schemaname \
    ,tablename \
    ,"'"column"'" as columnname \
    ,encoding \
    from pg_table_def \
    where schemaname='"+schemaName+"' \
    and tablename<>'encoding_control'"

    result = execute_query(clusterid, db, secret, listTables)
    if(result["Records"] != []):
        for i in tqdm (range (1), desc="Loading Control Table for Schema {0}..".format(schemaName)):
            for datarecord in result["Records"]:
                loadControlQuery="insert into public.encoding_control \
                values("
                for column in datarecord:
                    loadControlQuery+="'"+column['stringValue']+"',"
                
                loadControlQuery+="default,default,default,default,current_timestamp);"
                execute_query(clusterid, db, secret, loadControlQuery)
    else:
        sys.exit("The schema does not exist or does not have any tables")

def is_table_large(clusterid, db, secret, schema, table, threshold):
    tableSizeQuery="select "'"schema"'" as schemaname \
    ,"'"table"'" as tablename \
    ,size as sizeinMB \
    FROM SVV_TABLE_INFO \
    where schemaname = '"+schema+"' \
    and tablename = '"+table+"';"

    result = execute_query(clusterid, db, secret, tableSizeQuery)
    if(result["Records"] != []):
        tablesize=result["Records"][0][2]['longValue']
        if((tablesize/1024/1024)>int(threshold)):
            return True
    
    return False

def first_sort_key(clusterid, db, secret, schema, table):
    sortKeyQuery="select "'"column"'" \
    FROM pg_table_def \
    where schemaname = '"+schema+"' \
    and tablename = '"+table+"' \
    and sortkey=1;"

    result = execute_query(clusterid, db, secret, sortKeyQuery)
    if(result["Records"] != []):
        return result["Records"][0][0]['stringValue']
    
    return ''



def update_control_table(clusterid, db, secret, schema_name, table_name, column_name, encoding, est_red_pct, analyze_ind, alter_status):
    updateQuery="update public.encoding_control \
    set recmd_encoding_type='"+encoding+"',   \
    est_red_pct='"+est_red_pct+"',   \
    analyze_ind='"+analyze_ind+"',    \
    alter_status='"+alter_status+"',  \
    update_timestamp=current_timestamp   \
    where schema_name='"+schema_name+"' \
    and table_name='"+table_name+"'    \
    and column_name='"+column_name+"';"

    result = execute_query(clusterid, db, secret, updateQuery)
    return result


def analyze_compression(clusterid, db, secret, schema, table):
    analyzeQuery='analyze compression '+schema+'.'+table+';'
    result = execute_query(clusterid, db, secret, analyzeQuery)
    if(result != None):
        return result["Records"]
    
    return result

def alter_compression(clusterid, db, secret, schema, table, column, encoding):

    alterQuery=" alter table "+schema+"."+table+" alter column "+column+" encode "+encoding+";"
    result = execute_query(clusterid, db, secret, alterQuery)
    return result

def get_control_table(clusterid, db, secret, schema_name):
    
    getTableQuery="select distinct table_name \
    from public.encoding_control    \
    where schema_name='"+schema_name+"';"
    
    result = execute_query(clusterid, db, secret, getTableQuery)
    if(result["Records"] != []):
        return result["Records"]
    

def compress_all(clusterid, db, secret,schema_name):
    result=get_control_table(clusterid, db, secret, schema_name)
    if(result != None):
        for i in tqdm (range (1), desc="Applying Compression for all Required Tables in Schema {0}..".format(schema_name)):
            for table in result:
                table_name=table[0]["stringValue"]
                fSortKey=first_sort_key(clusterid, db, secret, schema_name,table_name)
                analyze_result=analyze_compression(clusterid, db, secret, schema_name, table_name)
                if(analyze_result != None):
                    for encoding in analyze_result:
                        if(float(encoding[3]["stringValue"])>0 and encoding[1]["stringValue"]!=fSortKey):
                            alter_compression(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"])
                            update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'Y')
                        else:
                            update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'N')

    
def compress_large(clusterid, db, secret,schema_name,threshold):
    result=get_control_table(clusterid, db, secret, schema_name)
    if(result != None):
        for i in tqdm (range (1), desc="Applying Compression for Required Tables > {0} TB in Schema {1}..".format(threshold,schema_name)):
            for table in result:
                table_name=table[0]["stringValue"]
                fSortKey=first_sort_key(clusterid, db, secret, schema_name,table_name)
                if(is_table_large(clusterid, db, secret, schema_name, table_name, threshold)):
                    analyze_result=analyze_compression(clusterid, db, secret, schema_name, table_name)
                    if(analyze_result != None):
                        for encoding in analyze_result:
                            if(float(encoding[3]["stringValue"])>0 and encoding[1]["stringValue"]!=fSortKey):
                                alter_compression(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"])
                                update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'Y')
                            else:
                                update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'N')  

def compress_small(clusterid, db, secret,schema_name, threshold):
    result=get_control_table(clusterid, db, secret, schema_name)
    if(result != None):
        for i in tqdm (range (1), desc="Applying Compression for Required Tables < {0} TB in Schema {1}..".format(threshold,schema_name)):
            for table in result:
                table_name=table[0]["stringValue"]
                fSortKey=first_sort_key(clusterid, db, secret, schema_name,table_name)
                if(not is_table_large(clusterid, db, secret, schema_name, table_name, threshold)):
                    analyze_result=analyze_compression(clusterid, db, secret, schema_name, table_name)
                    if(analyze_result != None):
                        for encoding in analyze_result:
                            if(float(encoding[3]["stringValue"])>0 and encoding[1]["stringValue"]!=fSortKey):
                                alter_compression(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"])
                                update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'Y')
                            else:
                                update_control_table(clusterid, db, secret, schema_name, table_name, encoding[1]["stringValue"], encoding[2]["stringValue"], encoding[3]["stringValue"], 'Y', 'N')

        
        
    
    

def main(argv):
    
    try:
        if(len(sys.argv) == 1):
            print(
                '\nUsage: Alter-Compression-Utility.py   \
                \n\tThe utility can automatically analyze schema, identify tables and alter column encoding compression leveraging Alter Compression feature of Redshift released in Oct, 20   \
                \n\t\tReference Document: https://aws.amazon.com/about-aws/whats-new/2020/10/amazon-redshift-supports-modifying-column-comprression-encodings-to-optimize-storage-utilization-query-performance/ \
                \n\nArguments:\n \
                \n\targv[1] - Cluster Identifier: Name of the Cluster \
                \n\targv[2] - Database Name: Default database name that is required for connecting to cluster \
                \n\targv[3] - Secret Identifier: Secret ARN/Friendly name of the secret for retrieving user and password of the cluster \
                \n\targv[4] - Schema Name: Name of the schema that user wants to analyze and compress tables for  \
                \n\targv[5] - Compression Mode: Three modes of compression for users to choose from and pass as argument \
                \n\n\t\tcompress-all: Automatically analyze and compress required columns for all the tables for the schema    \
                \n\t\tcompress-small: Based on provided threshold value, Automatically analyze and compress required columns for tables < threshold value in TB for the schema   \
                \n\t\tcompress-large: Based on provided threshold value,  Automatically analyze and compress required columns for tables > threshold value in TB for the schema  \
                \n\n\targv[6] - Threshold value: Threshold value upon which small/large table size in TBs will be defined.The value is not required for compress-all mode.\n'
            )

        elif(len(sys.argv) > 7):
            print('\n\tPassed arguments have exceeded the limit. Please refer to the utility documentation\n')

        else:
            print('\n\n')
            if (argv[5]=='compress-all'):
                create_control_table(argv[1], argv[2], argv[3])
                truncate_control_table(argv[1], argv[2], argv[3])
                load_control_table(argv[1], argv[2], argv[3], argv[4])
                compress_all(argv[1], argv[2], argv[3],argv[4])
                print("\nComplete: \n\tCompression has successfully been altered for all required tables in schema {0} and public.encoding_control has been updated for detail. \n".format(argv[4]))

            elif(int(argv[6])>0 and (argv[5]=='compress-small' or argv[5]=='compress-large')):

                create_control_table(argv[1], argv[2], argv[3])
                truncate_control_table(argv[1], argv[2], argv[3])
                load_control_table(argv[1], argv[2], argv[3], argv[4])
                if(argv[5]=='compress-small'):
                    compress_small(argv[1], argv[2], argv[3],argv[4],argv[6])
                    print("\nComplete: \n\tCompression has successfully been altered for all required tables < {0} TB in schema {1} and public.encoding_control has been updated for detail. \n".format(argv[6], argv[4]))

                elif(argv[5]=='compress-large'):
                    compress_large(argv[1], argv[2], argv[3],argv[4],argv[6])
                    print("\nComplete: \n\tCompression has successfully been altered for all required tables > {0} TB in schema {1} \
                    and public.encoding_control has been updated for detail. \n".format(argv[6], argv[4]))

            else:
                if (argv[5]!='compress-all' and int(argv[6])<=0):
                    print("\nPlease pass a table size (TB) threshold value greater than zero. Refer to the utility documentation !\n")
                else:
                    print("\nPlease pass a valid compression mode. Refer to the utility documentation !\n")

    except IndexError as e:
        print("\nPlease pass valid command line arguments. Refer to the utility documentation ! \
        \n\n\tIndex Error: {0}\n".format(e))
    except:
        print('\n\tAn error has occurred - error reason', sys.exc_info()[0])
        print('\n\tThe error details 1: {0}'.format(sys.exc_info()[1]))
        print('\n\tThe error details 2:{0} \n'.format(sys.exc_info()[2]))


#...........................
# set the main function
#...........................
if __name__ == "__main__":
    main(sys.argv[0:])