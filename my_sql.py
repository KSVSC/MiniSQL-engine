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

create_database()
print(data_dict)
