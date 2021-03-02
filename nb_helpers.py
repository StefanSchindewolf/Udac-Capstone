# Modules and Filename config
import pandas as pd
import numpy as np
import logging
import sys


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
    return data_dict


def summarize_data(df, type_choice=None, examples=10):
    """ DOC STRING"""
    # Set number of examples to be printed per value
    # examples = 10
    
    # If type_choice is set, only the dtypes provided will be analysed
    if type_choice is None:
        # If not set, we simple analyze numeric and string data and
        # print the result
        type_choice = ['all']
        print('Running Data Quantifier with parameter: ', ', '.join(type_choice),\
             ' and example threshhold is ', examples)
    else:
        print('Running Data Quantifier with parameter: ',  ', '.join(type_choice),\
             ' and example threshhold is ', examples)
    
    # Analysis section
    if (('all' in type_choice) or ('numbers' in type_choice)):
        # NUMERIC DATA ANALYSIS
        sub_df = df.select_dtypes(exclude=['object'])
        print('\nQuantifying NUMERIC data types in columns:\n',  ', '.join(sub_df.columns), '\n')
        # Get descriptive statistics
        stat_df = sub_df.describe()
        # Count missing values per column
        miss_df = pd.DataFrame.from_dict({'Missing': sub_df.isna().sum()})
        #miss_df = miss_df['Missing'].astype(int)
        #mis_val_cols = miss_df.loc[miss_df['Missing'] > 0].columns
        mis_val_cols = miss_df[miss_df > 0].dropna().index
        # Count unique values per column
        uniq_df = pd.DataFrame.from_dict({'Unique': sub_df.nunique()})
        #uniq_df = uniq_df['Unique'].astype(int)
        # Get list of example values for columns which have less than x unique values
        uni_val_cols = uniq_df[uniq_df <= examples].dropna().index
        uniq_df = uniq_df.transpose()
        miss_df = miss_df.transpose()
        stat_df = pd.concat([stat_df, uniq_df, miss_df])
        display(stat_df)
        print('Columns with missing values: ', ','.join(mis_val_cols), '\n')
        for unique_value_column in uni_val_cols:
            unique_values = df[unique_value_column].drop_duplicates()
            msg = 'Unique values in column \'{}\': \n'.format(unique_value_column)
            print(msg, unique_values.values, '\n')

    if (('all' in type_choice) or ('object' in type_choice)):
        # STRING DATA ANALYSIS
        sub_df = df.select_dtypes(exclude=['float64'])
        print('\nQuantifying NON-NUMERIC data types in columns:\n',  ', '.join(sub_df.columns))
        stat_df = pd.DataFrame.from_dict(data=dict(sub_df.dtypes), orient='index', columns=['Datatype'])
        stat_df['Lines'] = len(df)
        stat_df['Non-Null'] = df.count()
        stat_df['NaN'] = df.isna().sum()
        stat_df['Fill-%'] = df.count() / len(df) *100
        stat_df['Unique'] = df.nunique()
        stat_df['Uniq-%'] = stat_df['Unique'] / stat_df['Lines'] *100
        mis_val_cols = list(stat_df.loc[stat_df['Fill-%'] < 100].index)
        uni_val_cols = list(stat_df.loc[stat_df['Unique'] <= examples].index)
        display(stat_df.transpose())
        print('Columns with missing values: ', ','.join(mis_val_cols), '\n')
        for unique_value_column in uni_val_cols:
            unique_values = df[unique_value_column].drop_duplicates()
            msg = 'Unique values in column \'{}\': \n'.format(unique_value_column)
            print(msg, unique_values.values, '\n')
    print("\n\nData Quantification Done\n\n")
    return 0


def read_sas_in_chunks(fname=0, fformat=0, max_lines=0, steps=0):
    """DOC STRING"""
    from os.path import getsize
    import pandas as pd
    #if 0 in ['fname', 'fformat', 'max_lines', 'steps']:
    sas_size = int(getsize(fname) / 1024 / 1024)
    return_df = pd.DataFrame()
    lines_imported = 0
    print('START reading SAS file {} of (Filesize: {} Mb)'.format(fname, sas_size))
    # The method _read_sas()_ will read the files in chunks
    sas_reader = pd.read_sas(fname, fformat, encoding='ISO-8859-1', chunksize=steps)
    for chunk in sas_reader:
        last_lines = lines_imported + 1
        lines_imported = lines_imported + len(chunk)
        return_df = return_df.append(chunk)
        print('\t\t\tImporting lines from {} to {} of total {} lines'.format(last_lines, lines_imported, max_lines))
        if lines_imported >= max_lines:
            print('STOP reading SAS files\n')
            break
            
    # Output and return
    print('First lines of data and data types:')
    print(return_df.head())
    return_df_typ = pd.DataFrame(return_df.dtypes).transpose()
    print(return_df_typ)
    return return_df


if __name__ == "__main__":
    main()