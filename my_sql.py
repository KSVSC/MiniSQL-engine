import sys
import re

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
        print("query input is wrong")
        return 1
    if bool(re.match('^select.*from.*', query)) is False:
        print("query input is wrong")
        return 1
    return 0


def check_table(attributes, tables):
    for attr in attributes:
        counter = 0
        if len(attr.split('.')) == 2:
            table = attr.split('.')[0]
            if attr.split('.')[1] not in data_dict[table]['attributes']:
                print("Invalid field", attr)
                return 0
        else:
            for table in tables:
                if attr in data_dict[table]['attributes']:
                    counter += 1
            if counter != 1:
                print("Inconsistent attribute", attr)
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
            if eval(cond):
                result['values'].append(row)
    else:
        for row in join_table['values']:
            result['values'].append(row)

    return result


def display(table):
    print("RESULT")
    print("___________________________________________________________")
    print(' || ',' || '.join(table['attributes']))
    print("|___________________________________________________________|")
    for row in table['values']:
        print(' || ',' || '.join([str(x) for x in row]))
        print("|-----------------------------------------------------------|")


def result_query(table, attr, dist_cond, aggr_func):
    result_table = {}
    result_table['attributes'] = []
    result_table['values'] = []

    if aggr_func is not None:
        result_table['attributes'].append(aggr_func + "(" + attr[0] + ")")
        coln_index = table['attributes'].index(attr[0])

        temp = []
        for row in table['values']:
            temp.append(row[coln_index])

        aggregate_functions = {'sum': sum(temp), 'avg': (sum(temp) * 1.0) / len(temp), 'max': max(temp), 'min': min(temp)}
        result_table['values'].append([aggregate_functions[aggr_func]])
    
    else:
        if attr[0] == '*':
            temp = []
            for x in table['attributes']:
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
            ind = table['attributes'].index(field)
            coln_indices.append(ind)

        for row in table['values']:
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
    aggr_func = None
    all_attr = False

    if query_error(query):
        return

    query = query.strip(';')
    select_query = query.split('from')[0]
    select_query = (select_query.replace('select', '')).strip()

    if bool(re.match('^distinct.*', select_query)) is True:
        dist_cond = True
        select_query = (select_query.replace("distinct", '')).strip()

    if bool(re.match('^(sum|avg|max|min)\(.*\)', select_query)) is True:
        aggr_func = (select_query.split('(')[0]).strip()
        select_query = (select_query.replace(aggr_func, '')).strip()
        select_query = select_query.strip('()')

    select_query = select_query.split(',')
    for i in range(len(select_query)):
        select_query[i] = select_query[i].strip()

    if len(select_query) == 1:
        if select_query[0] == '*':
            all_attr = True

    if aggr_func is not None and len(select_query) > 1:
        print("Invalid params in argument")
        return

    from_query = query.split('from')[1]
    from_query = (from_query.split('where')[0]).strip()
    from_query = from_query.split(',')

    for i in range(len(from_query)):
        from_query[i] = from_query[i].strip()

    for table in from_query:
        if table not in data_dict:
            print("Invalid table - ", table)
            return

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
        
        if all_attr is False:
            for i in range(len(select_query)):
                if len(select_query[i].split('.')) == 1:
                    for t in from_query:
                        if select_query[i] not in data_dict[t]['attributes']:
                            continue
                        else:
                            select_query[i] = t + '.' + select_query[i]
                            break

        for attr in condition_attr:
            if len(attr.split('.')) == 1:
                for t in from_query:
                    if attr not in data_dict[t]['attributes']:
                        continue
                    else:
                        temp1 = t + '.' + attr
                        temp2 = ' ' + where_query
                        where_query = re.sub('(?<=[^a-zA-Z0-9])(' + attr + ')(?=[\(\)= ])', temp1, temp2)
                        where_query = where_query.strip(' ')

        display(result_query(project(from_query, where_query), select_query, dist_cond, aggr_func))
        
    else:
        if (len(from_query)) >= 2:
            if all_attr is True:
                display(result_query(project(from_query,1), select_query, dist_cond, aggr_func))
                return
            if len(select_query) <= 1:     
                print("Too many arguments for tables")
                return
        if all_attr is not True:
            for field in select_query:
                if field in data_dict[from_query[0]]['attributes']:
                    continue
                else:
                    print("Invalid attribute", field)
                    return

        display(result_query(data_dict[from_query[0]], select_query, dist_cond, aggr_func))


def main():
    create_database()
    query = sys.argv[1]
    parse_query(query)


if __name__ == '__main__':
    main()
