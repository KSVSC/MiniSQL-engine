import sys
import re

data_dict = {}
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

def check_table(attr, tables):
    for field in attr:
        # counter = 0
        if len(field.split('.') == 2):
            table = field.split('.')[0]
            if field.split('.')[1] in data_dict[table]['attributes']:
                return 0
    print("Invalid attribute" + field)
    return 1
    #     for table in tables:
    #         if field.split('.')[1] in data_dict[table]['attributes']:
    #             if len(field.split('.')) == 2 and field.split('.')[0] == table:
    #                 field_count = field_count + 1
    #             else:
    #                 field_count = field_count + 1

    #     if field_error(field_count):
    #         return 0
    # return 1


def parse_query(query):

    query = query.replace("SELECT", "select").replace("FROM", "from").replace("WHERE", "where").replace("DISTINCT", "distinct").replace("AND ", "and ").replace("OR ", "or ").replace("MIN", "min").replace("MAX", "max").replace("AVG", "avg").replace("SUM", "sum")

    dist_cond = False   
    aggr_func = []
    all_attr = False

    if query_error(query):
        return

    query = query.replace(";","")
    query = query.replace('select', '')
    select_query = (query.split('from')[0]).strip() 
    
    if bool(re.match('^distinct.*', select_query)) is True:
        dist_cond = True
        select_query = (select_query.replace("distinct", '')).strip()
        
    select_query = select_query.split(',')
    for i in range(len(select_query)):
        select_query[i] = select_query[i].strip()

        if bool(re.match('^(sum|avg|max|min)\(.*\)', select_query[i])) is True:
            aggr_func[i] = select_query.split('(')[0]
            select_query = (select_query.replace(aggr_func, '')).strip()
            select_query = select_query.strip('()')

    if len(select_query) == 1:
        select_query[0] = '*'
        all_attr = True

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
        temp = where_query.replace("and", '')
        temp = temp.replace("or", '')

        condition_attr = re.findall(r"[a-zA-Z][\w\.]*", temp)
        condition_attr = list(condition_attr)

        if check_table(condition_attr, from_query) == 0:
            return
        
        if all_attr is False:
            for i in range(len(select_query)):
                if len(select_query[i].split('.')) == 1:
                    for t in from_query:
                        coln_append = t + '.'
                        if select_query[i] not in data_dict[t]['attributes']:
                            continue
                        else:
                            select_query[i] = coln_append + select_query[i]
                            break

        for attr in condition_attr:
            if len(attr.split('.')) == 1:
                for t in from_query:
                    if attr not in data_dict[t]["attributes"]:
                        continue
                    else:
                        temp1 = t + '.' + attr
                        temp2 = ' ' + where_query
                        where_query = re.sub('(?<=[^a-zA-Z0-9])(' + attr + ')(?=[\(\)= ])', temp1, temp2)
                        where_query = where_query.strip(' ')

        display(result(select(from_query, where_query), select_query, dist_cond, aggr_func))
        
    else:
        if (len(from_query)) >= 2:
            print("more arguments for tables")
            return
        
        if all_attr is not True:
            for field in select_query:
                if field in data_dict[from_query[0]]["attributes"]:
                    continue
                else:
                    print("Invalid attribute" + field)
                    return

        display(result(data_dict[from_query[0]], select_query, dist_cond, aggr_func))


def main():
    create_database()
    query = sys.argv[1]
    parse_query(query)

if __name__ == '__main__':
    main()
