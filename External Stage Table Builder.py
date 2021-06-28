from os import listdir
from os.path import isfile, join
from os import linesep
import re

# Edit the below values.

delimiter = '<your CSV delimiter (one character)>'
path_to_files = r'<specify the filepath where your staged files are located>'
stage_name = '<name of your stage in Snowflake>'
file_format_name = '<name of the file format you will use >'
database_name = 'DEMO'
schema_name = 'BOOKSHOP'
save_location = r'C:\Users\Christopher Marland\Desktop\Snowflake'

# Do not edit the below unless you know what you're doing.

staged_files_list = [f for f in listdir(path_to_files) if isfile(join(path_to_files, f))]
concat_all_queries = ''
    
for file in staged_files_list:
    
    with open(path_to_files + '\\' + file, "r") as opened_file:
        headers = opened_file.readline().split(delimiter)
        count = 0
        num_of_headers_in_file = len(headers)
        concat = ''
        
        
        for header in headers:
            count += 1
            header = header.replace('\n', '').replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
            
            if count == 1:
                header = 'SELECT t.$' + str(count) + ' AS ' \
                + header + ','
            elif count == num_of_headers_in_file:
                header = '       t.$' + str(count) + ' AS ' \
                + header
            else:
                header = '       t.$' + str(count) + ' AS ' \
                + header + ','
                
            concat += linesep + header
            
            
        query_per_file = 'CREATE OR REPLACE TABLE {}.{}.{} AS'.format(database_name, 
                                                                      schema_name, 
                                                                      file.replace('.csv', '').replace(' ', '_') \
                                                                      .upper()) + \
        concat + linesep + 'FROM @{} (file_format=> {}, pattern=>".*{}") t;'.format(stage_name,
                                                                                    file_format_name,
                                                                                    file) + linesep + linesep
    concat_all_queries += query_per_file 

        

concat_all_queries = re.sub('\n{1}', '', concat_all_queries)

with open(save_location + '\\' + 'query.txt', 'w') as f:
    f.write(concat_all_queries)