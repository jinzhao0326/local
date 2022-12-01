import re

def find_col_definition_range(ddl:str) -> tuple:
    
    """
    A function to parse a snowflake DDL and find the indcies of the brackets that contain column level definition
    """
    
    stack = []

    i_start = ddl.find('(')
    i_end = None
    
    i = i_start
    if i_start != -1:
        for c in ddl[i_start:]:
            if c == '(':
                stack.append(c)
            elif c == ')' and stack[-1] == '(':
                stack.pop()
            elif c == ')' and stack[-1] != '(':
                raise Exception ('Does not appear to be a valid Snowflake DDL.')

            if len(stack) == 0:
                i_end = i
                return (i_start, i_end)
            i += 1    
    else:
        raise Exception ('Does not appear to be a valid Snowflake DDL.')
    
    raise Exception ('Does not appear to be a valid Snowflake DDL.')

def table_column_position(ddl:str) -> dict:
    """
    Return a dictionary in which the key in the k-v pairs is the name of a column in a snowflake DDL and the value in the k-v pairs is the index/position of the column
    """
    col = {}
    col_index = 0

    ddl_parsed = re.sub(r"COMMENT ?'(.*?)'", '', ddl[find_col_definition_range(ddl)[0]+1:find_col_definition_range(ddl)[1]].replace('\n', '').replace('\t', ''))

    for c in ddl_parsed.split(', '):
        if c.split()[0] not in col:
            col[c.split()[0]] = col_index
            col_index += 1
        else:
            raise Exception ('Column name appearing multiple times: {}'.format(c.split()[0]))

    return col

def table_column_type(ddl:str) -> dict:
    """
    Return a dictionary in which the key in the k-v pairs is the name of a column in a snowflake DDL and the value in the k-v pairs is the data type of the column
    """
    col = {}

    ddl_parsed = re.sub(r"COMMENT ?'(.*?)'", '', ddl[find_col_definition_range(ddl)[0]+1:find_col_definition_range(ddl)[1]].replace('\n', '').replace('\t', ''))

    for c in ddl_parsed.split(', '):
        if c.split()[0] not in col:
            col[c.split()[0]] = c.split()[1]
        else:
            raise Exception ('Column name appearing multiple times: {}'.format(c.split()[0]))

    return col



# Can be optimized
def compare_column_type_index(new_ddl:str, old_ddl:str) -> bool:
    """
    Compare the column indexing and column typing of 2 snowflake DDL.
    """
    new_col_list = {}
    new_col_index = 0

    old_col_list = {}
    old_col_index = 0

    new_ddl_parsed = re.sub(r"'(.*?)'", '', new_ddl[find_col_definition_range(new_ddl)[0]+1:find_col_definition_range(new_ddl)[1]].replace('\n', '').replace('\t', ''))
    old_ddl_parsed = re.sub(r"'(.*?)'", '', old_ddl[find_col_definition_range(old_ddl)[0]+1:find_col_definition_range(old_ddl)[1]].replace('\n', '').replace('\t', ''))

    for c in new_ddl_parsed.split(', '):
        if c.split()[0] not in new_col_list:
            new_col_list[c.split()[0]] = {'type': c.split()[1], 'index': new_col_index}
            new_col_index += 1
        else:
            raise Exception ('Column name appearing multiple times: {}'.format(c.split()[0]))

    for c in old_ddl_parsed.split(', '):
        if c.split()[0] not in old_col_list:
            old_col_list[c.split()[0]] = {'type': c.split()[1], 'index': old_col_index}
            old_col_index += 1
        else:
            raise Exception ('Column name appearing multiple times: {}'.format(c.split()[0]))

    if len(new_col_list) != len(old_col_list):
        raise Exception ('Column counts do not match.')
    for c in new_col_list:
            if new_col_list[c]['type'].upper() != old_col_list[c]['type'].upper() or new_col_list[c]['index'] != old_col_list[c]['index']:
                raise Exception('Found a mismatch for column {} in the new ddl'.format(c))

    return True

