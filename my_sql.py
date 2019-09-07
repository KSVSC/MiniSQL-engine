import sys
import re
import copy

data_dict = {}
all_conditions = []


def create_database():
    f = open("./metadata.txt",'r')
    line = f.readline().strip()

    while line:
        if line == "<begin_table>" :
            t_name = f.readline().strip()
            data_dict[t_name] = {}
            data_dict[t_name]['name'] = t_name
            data_dict[t_name]['attributes'] = []
            attribute = f.readline().strip()
            while attribute != "<end_table>":
                data_dict[t_name]['attributes'].append(attribute)
                attribute = f.readline().strip()
        line = f.readline().strip()

    for t_name in data_dict:
        data_dict[t_name]['values'] = []
        f = open('./' + t_name + '.csv', 'r')
        for line in f:
            data_dict[t_name]['values'].append([int(field.strip('"')) for field in line.strip().split(',')])


def query_error(query):
    if query[len(query) - 1] != ';':
        print("Error: Semicolon missing")
        return 1
    if bool(re.match('^select.*from.*', query)) is False:
        print("Error: Input query is wrong")
        return 1
    if bool(re.match('^select\s(sum|avg|max|min)\(.*\w\,.*\w\).*from.*', query)) is True:
        print("Error: Invalid arguments-can't take more than one parameter")
        return 1
    return 0


def check_table(attributes, tables):
    for attr in attributes:
        counter = 0
        if len(attr.split('.')) == 2:
            table = attr.split('.')[0]
            if attr.split('.')[1] not in data_dict[table]['attributes']:
                print("Error: Invalid attribute", attr)
                return 0
        else:
            for table in tables:
                if attr in data_dict[table]['attributes']:
                    counter += 1
            if counter > 1:
                print("Error: Inconsistent attribute", attr)
                return 0
            elif counter == 0:
                print("Error: Invalid attribute", attr)
                return 0
    return 1


def join_cond(t1, t2):
    cross_table = {}
    cross_table['attributes'] = []
    cross_table['values'] = []
    
    temp1 = []
    for attr in t1['attributes']:
        if len(attr.split('.')) == 1:
            temp1.append(t1['name'] + '.' + attr)
        else:
            temp1.append(attr)

    temp2 = []
    for attr in t2['attributes']:
        if len(attr.split('.')) == 1:
            temp2.append(t2['name'] + '.' + attr)
        else:
            temp2.append(attr)

    cross_table['attributes'] = cross_table['attributes'] + temp1 + temp2

    for row1 in t1['values']:
        for row2 in t2['values']:
            cross_table['values'].append(row1 + row2)

    return cross_table


def project(tables, cond):
    result = {}
    result['attributes'] = []
    result['values'] = []
    
    if len(tables) == 1:
        join_table = join_cond(data_dict[tables[0]], {'attributes': [], 'values': [[]]})
    else:
        join_table = join_cond(data_dict[tables[0]], data_dict[tables[1]])

    if len(tables) > 2:
        for i in range(len(tables) - 2):
            join_table = join_cond(join_table, data_dict[tables[i + 2]])

    for x in join_table['attributes']:
        result['attributes'].append(x)

    if cond != 1:    
        cond = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', cond)
        cond_str = cond.replace("and", ",")
        cond_str = cond_str.replace("or", ",").replace("(", '').replace(")", "")
        cond_str = cond_str.split(',')

        for condition in cond_str:
            if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):
                temp1 = condition.strip()
                temp1 = (temp1.split("==")[0]).strip()

                temp2 = condition.strip()
                temp2 = (temp2.split("==")[1]).strip()

                joint_cond = (temp1, temp2)
                all_conditions.append(joint_cond)

        for attr in join_table['attributes']:
            cond = cond.replace(attr, 'row[' + str(join_table['attributes'].index(attr)) + ']')

        for row in join_table['values']:
            try:
                eval(cond)
            except:
                print('Error: Condition not valid')
                exit()
            if eval(cond):
                result['values'].append(row)
    else:
        for row in join_table['values']:
            result['values'].append(row)

    return result


def display(table):
    print("Output:")
    print(','.join(table['attributes']))
    for row in table['values']:
        print(','.join([str(x) for x in row]))


def result_query(table1, attr, dist_cond, aggr_func, aggr):
    result_table = {}
    result_table['attributes'] = []
    result_table['values'] = []

    for l in range(len(attr)):
        if len(attr[l].split('.')) == 1:
            attr[l] = table1['name'] + '.' + attr[l]
                    
    for i in range(len(table1['attributes'])):
        if len(table1['attributes'][i].split('.')) == 1:
            table1['attributes'][i] = table1['name'] + '.' + table1['attributes'][i]

    if aggr_func != 0:
        for i in range(aggr_func):
            result_table['attributes'].append(aggr[i] + "(" + attr[i] + ")")
            try:
                coln_index = table1['attributes'].index(attr[i])
            except:
                print("Error: myNot valid")
                exit()

            temp = []
            for row in table1['values']:
                temp.append(row[coln_index])

            aggregate_functions = {'sum': sum(temp), 'avg': (sum(temp) * 1.0) / len(temp), 'max': max(temp), 'min': min(temp)}
            result_table['values'].append([aggregate_functions[aggr[i]]])
    
    else:
        if attr[0] == '*':
            temp = []
            for x in table1['attributes']:
                temp.append(x)
            attr[:] = temp[:]
            for colns in all_conditions:
                temp[:] = []
                for x in attr:
                    if x != colns[1]:
                        temp.append(x)
                attr[:] = temp[:]
        result_table['attributes'] += attr
        coln_indices = []

        for field in attr:
            try:
                ind = table1['attributes'].index(field)
            except:
                print("Error: Not valid")
                exit()
            coln_indices.append(ind)

        for row in table1['values']:
            result_row = []
            for i in coln_indices:
                result_row.append(row[i])
            result_table['values'].append(result_row)

        if dist_cond is True:
            temp = sorted(result_table['values'])
            result_table['values'][:] = []
            for i in range(len(temp)):
                if i == 0 or temp[i] != temp[i - 1]:
                    result_table['values'].append(temp[i])

    return result_table


def parse_query(query):
    query = query.strip('"').strip()
    query = query.replace("SELECT", "select").replace("FROM", "from").replace("WHERE", "where").replace("DISTINCT", "distinct").replace("AND ", "and ").replace("OR ", "or ").replace("MIN", "min").replace("MAX", "max").replace("AVG", "avg").replace("SUM", "sum")

    dist_cond = False   
    aggr_func = 0
    aggr = []
    all_attr = False

    if query_error(query):
        return

    query = query.strip(';')
    select_query = query.split('from')[0]
    select_query = (select_query.replace('select', '')).strip()

    if bool(re.match('^distinct.*', select_query)) is True:
        dist_cond = True
        select_query = (select_query.replace("distinct", '')).strip()

    select_query = select_query.split(',')
    for i in range(len(select_query)):
        select_query[i] = select_query[i].strip()
        if bool(re.match('^(sum|avg|max|min)\(.*\)', select_query[i])) is True:
            aggr.append(select_query[i].split('(')[0])
            aggr_func += 1
            select_query[i] = (select_query[i].replace(aggr[i], '')).strip()
            select_query[i] = select_query[i].strip('()')

    if len(select_query) == 1:
        if select_query[0] == '*':
            all_attr = True

    from_query = query.split('from')[1]
    from_query = (from_query.split('where')[0]).strip()
    from_query = from_query.split(',')

    for i in range(len(from_query)):
        from_query[i] = from_query[i].strip()

    for table in from_query:
        if table not in data_dict:
            print("Error: Invalid table", table)
            return

    field_query = copy.deepcopy(select_query)
    if all_attr is False:
        for i in range(len(select_query)):
            count1 = 0
            t1 = None
            if len(select_query[i].split('.')) == 1:
                for t in from_query:
                    if select_query[i] in data_dict[t]['attributes']:
                        count1 += 1
                        t1 = t
                if count1 == 1:
                    select_query[i] = t1 + '.' + select_query[i]
                    
    if bool(re.match('^select.*from.*where.*', query)):
        if all_attr == False:
            if check_table(select_query, from_query) == 0:
                return
    
        where_query = query.split('where')[1]
        where_query = where_query.strip()
        temp = where_query.replace(' and ', ' ')
        temp = temp.replace(' or ', ' ')

        condition_attr = re.findall(r"[a-zA-Z][\w\.]*", temp)
        condition_attr = set(condition_attr)
        condition_attr = list(condition_attr)

        if check_table(condition_attr, from_query) == 0:
            return

        attr_var = copy.deepcopy(condition_attr)
        for i in range(len(condition_attr)):
            if len(condition_attr[i].split('.')) == 1:
                for t in from_query:
                    if condition_attr[i] not in data_dict[t]['attributes']:
                        continue
                    else:
                        condition_attr[i] = t + '.' + condition_attr[i]
                
                where_query = where_query.replace(attr_var[i], condition_attr[i])

        display(result_query(project(from_query, where_query), select_query, dist_cond, aggr_func,aggr))
        
    else:
        if (len(from_query)) > 1:
            if all_attr is True:
                display(result_query(project(from_query,1), select_query, dist_cond, aggr_func,aggr))
                return
            else:
                if check_table(select_query, from_query) == 0:
                    return
                display(result_query(project(from_query,1), select_query, dist_cond, aggr_func,aggr))
                return
        else:
            if all_attr is not True:
                if check_table(field_query, from_query) == 0:
                    return
            display(result_query(data_dict[from_query[0]], field_query, dist_cond, aggr_func,aggr))


def main():
    create_database()
    query = sys.argv[1]
    parse_query(query)


if __name__ == '__main__':
    main()
