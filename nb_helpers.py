# Modules and Filename config
import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime

file_name = 'I94_SAS_Labels_Descriptions.SAS'

# Small and unreliable SAS config parser
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s \t %(message)s ',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger('log')

def get_sas_definitions(file):
    """ Checks a given .SAS file line by line searching for text patterns.
        Idenfified variables are collected in a dictionary.
        Value constraints are stored as <variable label>+'l'
        
        Returns:  dict with variables, variable discriptions 
                  and value constraints
    
    """
    # Empty dict to return
    data_dict = dict()
    # Flag if we found a single line variable description
    confline = False
    # Flag if we found a multiple line variable description
    multiline = False
    # Placeholder for variable name
    varname = ''
    
    with open(file,mode='r') as f:
        start_time = datetime.now()
        for line in f.read().splitlines():
            line = line.strip()
            # Recognize data variable descriptions by "/*" at the start of the line
            
            # If we have the separator at beginning and end then we can store a variable
            if ((line.startswith('/* ')) and (line.endswith(' */'))):
                multiline = False
                line = line.lstrip('/* ').rstrip(' */')
                splitted = line.split(sep='-')
                colnames = splitted[0].strip().lower()
                desc = splitted[1].strip()
                if '&' in colnames:
                    colnames = colnames.split(sep='&')
                    for cn in colnames:
                        cn = cn.strip()
                        data_dict[cn] = desc
                        log.info('Found Field Description for column {}: {}'.format(cn, desc))
                else:
                    data_dict[colnames] = desc
                    log.info('Found Field Description for column {}: {}'.format(colnames, desc))
            
            # If its just the start of a multiline description then store the first part
            # of the description and set the multiline flag
            elif line.startswith('/* '):
                multiline = True
                line = line.lstrip('/* ').rstrip()
                try:
                    splitted = line.split(sep='-', maxsplit=1)
                    colnames = splitted[0].strip().lower()
                    desc = splitted[1].strip()
                except:
                    splitted = line.split(sep=' ', maxsplit=1)
                    colnames = splitted[0].strip().lower()
                    desc = splitted[1].strip()
                if '&' in colnames:
                    colnames = colnames.split(sep='&')
                    for cn in colnames:
                        cn = cn.strip()
                        data_dict[cn.lower()] = desc
                        log.info('Found Field Description for column {}: {}'.format(cn, desc))
                else:
                    data_dict[colnames] = desc
                    log.info('Found Field Description for column {}: {}'.format(colnames, desc))
            else:
                # If a multiline description was started then just append the line
                if (multiline == True and line.endswith(' */') == True):
                    multiline = False
                    line = line.lstrip('/* ').rstrip()
                    if '&' in colnames:
                        colnames = colnames.split(sep='&').lower()
                        for cn in colnames:
                            cn = cn.strip()
                            data_dict[cn] = data_dict[cn] + line
                            log.info('Appending Field Description for column {}'.format(cn))
                    else:
                        data_dict[colnames] = data_dict[colnames] + desc
                        log.info('Appending Field Description for column {}'.format(colnames))
                # Except its the end of the multiline description
                elif (multiline == True and line.endswith(' */') == False):
                    multiline = True
                    line = line.lstrip('/* ').rstrip()
                    if '&' in colnames:
                        colnames = colnames.split(sep='&').lower()
                        for cn in colnames:
                            cn = cn.strip()
                            data_dict[cn] = data_dict[cn] + line
                            log.info('Appending Field Description for column {}'.format(cn))
                    else:
                        data_dict[colnames] = data_dict[colnames] + desc
                        log.info('Appending Field Description for column {}'.format(colnames))
                # The term "value" marks the beginning of a value constraint section
                elif line.startswith('value'):
                    confline = True
                    parameters = dict()
                    line = line.split(' ')
                    varname = line[1].rstrip('l')
                # In the section each constraint is assigned with a "="
                elif ('=' in line and confline == True):
                    line = line.split('=')
                    key = line[0].strip().strip('\'')
                    value = line[1].strip().strip('\'')
                    data_dict[(varname, key)] = value
                    logging.info('Getting parameter value for variable {}: {} {}'.format(varname, key, value))
                # An empty line marks the end of the constraint section
                elif len(line) == 0:
                    confline = False
                else:
                    next
    elapse = datetime.now() - start_time
    print('\n\nReading SAS definitions files DONE, time elapsed is {:7.2} sec\n\n'.format(elapse.total_seconds()))
    return data_dict


def summarize_data(df, type_choice=None, examples=10):
    """ DOC STRING"""
    # Set number of examples to be printed per value
    # examples = 10
    
    # If type_choice is set, only the dtypes provided will be analysed
    start_time = datetime.now()
    if type_choice is None:
        # If not set, we simple analyze numeric and string data and
        # print the result
        type_choice = ['all']
        display('''Running Data Quantifier with parameter: '''.format(type_choice), ''' and example threshhold is '''.format(examples))
    else:
        choices = ', '.join(type_choice)
        display('''Running Data Quantifier with parameter: '''.format(choices), ''' and example threshhold is '''.format(examples))
    
    # Quantify Dataframe with NUMERIC DATA
    if (('all' in type_choice) or ('numbers' in type_choice)):
        #  Create a subset with columns in scope           
        sub_df = df.select_dtypes(exclude=['object'])
        display('The dataframe has {} rows and {} columns. Godspeed!'.format(sub_df.shape[0], sub_df.shape[1]))
        display('Quantifying NUMERIC data types in columns:')
        display(', '.join(sub_df.columns))
        # Get descriptive statistics
        stat_df_num = sub_df.describe()
        # Count missing values per column
        miss_df_num = pd.DataFrame.from_dict({'Missing': sub_df.isna().sum()})
        miss_num = miss_df_num.loc[miss_df_num['Missing'] > 0].dropna().index
        # Count unique values per column
        uniq_df_num = pd.DataFrame.from_dict({'Unique': sub_df.nunique()})
        # Get list of example values for columns which have less than x unique values
        uniq_num = uniq_df_num.loc[uniq_df_num['Unique'] <= examples].dropna().index
        uniq_df_num = uniq_df_num.transpose()
        miss_df_num = miss_df_num.transpose()

    # Quantify Dataframe with STRING DATA
    if (('all' in type_choice) or ('object' in type_choice)):
        #  Create a subset with columns in scope
        sub_df = df.select_dtypes(exclude=['float64', 'int64'])
        display('The dataframe has {} rows and {} columns. Godspeed!'.format(sub_df.shape[0], sub_df.shape[1]))
        display('Quantifying NON-NUMERIC data types in columns:')
        display(', '.join(sub_df.columns))
        stat_df_str = pd.DataFrame.from_dict(data=dict(sub_df.dtypes), orient='index', columns=['Datatype'])
        # Count lines and missing values
        stat_df_str['Lines'] = len(df)
        stat_df_str['Non-Null'] = df.count()
        stat_df_str['NaN'] = df.isna().sum()
        stat_df_str['Fill-%'] = df.count() / len(df) *100
        # Count unique values per column
        stat_df_str['Unique'] = df.nunique()
        stat_df_str['Uniq-%'] = stat_df_str['Unique'] / stat_df_str['Lines'] *100
        mis_val_cols = list(stat_df_str.loc[stat_df_str['Fill-%'] < 100].index)
        uni_val_cols = list(stat_df_str.loc[stat_df_str['Unique'] <= examples].index)
        stat_df_str = stat_df_str.transpose()
    
    if 'numbers' in type_choice:
        stat_df = pd.concat([stat_df_num, uniq_df_num, miss_df_num])
        mis_val_cols = miss_num
        uni_val_cols = uniq_num
    elif 'object' in type_choice:
        stat_df = stat_df_str
    elif 'all' in type_choice:
        stat_df = pd.concat([stat_df_num, uniq_df_num, miss_df_num, stat_df_str])
        mis_val_cols = mis_val_cols.extend(miss_num)
        uni_val_cols = uni_val_cols.extend(uniq_num)
    else:
        pass
    
    # Output of results
    display(stat_df)
    display('''Columns with missing values:''')
    display(', '.join(mis_val_cols))
    display('''Columns with less than {} unique values:'''.format(examples))
    display(', '.join(uni_val_cols))
    for column in uni_val_cols:
        unique_values = df[column].drop_duplicates().dropna()
        if unique_values.dtype != object:
            output = ', '.join(["%10.0f" % x for x in unique_values])
        else:
            output = ', '.join(unique_values)
        display('Unique values in column \'{}\': '.format(column))
        display(output)
    elapse = datetime.now() - start_time
    display('Data Quantification Done, time elapsed is {:7.2} sec'.format(elapse.total_seconds()))
    return None


def read_csv_print(file, delim=";"):
    """ The Read CSV and Print function is a small wrapper around
        Pandas' read_csv() method.
        It requires the filename as input. If you have a file with a different
        separator than ";" you also need to provide this.
        Then it starts importing, prints a bit of context and
        Returns: Dataframe from CSV file
        """
    from os.path import getsize
    import pandas as pd
    from IPython.display import display
    start_time = datetime.now()
    fsize = getsize(file) / 1024 / 1024
    display('START reading CSV file {} of (Filesize: {:7.2} Mb)'.format(file, fsize))
    csv_df = pd.read_csv(file, sep=delim)
    display(csv_df.head())
    elapse = datetime.now() - start_time
    display('Done. Operation took {:7.2} seconds'.format(elapse.total_seconds()))
    return csv_df


def read_sas_in_chunks(fname=0, fformat=0, max_lines=0, steps=0):
    """DOC STRING"""
    from os.path import getsize
    import pandas as pd
    start_time = datetime.now()
    #if 0 in ['fname', 'fformat', 'max_lines', 'steps']:
    sas_size = int(getsize(fname) / 1024 / 1024)
    return_df = pd.DataFrame()
    lines_imported = 0
    display('''START reading SAS file {}, total filesize is: {} Mb'''.format(fname, sas_size))
    # The method _read_sas()_ will read the files in chunks
    sas_reader = pd.read_sas(fname, fformat, encoding='ISO-8859-1', chunksize=steps)
    for chunk in sas_reader:
        last_lines = lines_imported + 1
        lines_imported = lines_imported + len(chunk)
        return_df = return_df.append(chunk)
        display('''Importing lines from {} to {} of total {} lines'''.format(last_lines, lines_imported, max_lines))
        if lines_imported >= max_lines:
            display('STOP reading SAS files')
            break
            
    # Output and return
    return_df_typ = pd.DataFrame(return_df.dtypes).transpose()
    elapse = datetime.now() - start_time
    display('''First lines of data and data types:''')
    display(return_df.head())  
    display(return_df_typ)
    display('''DONE reading SAS data in chunks, time elapsed is {:7.2} seconds'''.format(elapse.total_seconds()))
    
    return return_df


if __name__ == "__main__":
    main()