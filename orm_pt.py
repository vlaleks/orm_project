import psycopg2

# команды

class Commands():
    database_settings = {}

    def __init__(self):
        self.name_table = ""
        super().__init__()

    def update_name_table(self, other): # смена таблицы
        self.name_table = other

    def get_cursor(self): # подключение базе данных
        connection = psycopg2.connect(**self.database_settings)
        connection.autocommit = True
        cursor = connection.cursor()
        
        return cursor

    def execute_query(self, query, params=None):  # выполнение команды
        self.cur = self.get_cursor()
        self.cur.execute(query, params)
    


    def select(self, *field_names, chunk_size=2000):
        fields_format = ', '.join(field_names)
        query = f"SELECT {fields_format} FROM {self.name_table}"

        # подключаемся к бд
        cursor = self.get_cursor()
        cursor.execute(query)

        model_objects = []
        is_fetching_completed = False
        while not is_fetching_completed:
            result = cursor.fetchmany(size=chunk_size)
            for row_values in result:
                keys, values = field_names, row_values
                row_data = dict(zip(keys, values))
                model_objects.append(row_data)
            is_fetching_completed = len(result) < chunk_size
        return model_objects

    def insert(self, rows: list):
        field_names = rows[0].keys()
        assert all(row.keys() == field_names for row in rows[1:])  # все строки содержат одинаковые поля

        fields_format = ", ".join(field_names)
        values_placeholder_format = ", ".join([f'({", ".join(["%s"] * len(field_names))})'] * len(rows))

        query = f"INSERT INTO {self.name_table} ({fields_format}) VALUES {values_placeholder_format}"

        params = []
        for row in rows:
            row_values = [row[field_name] for field_name in field_names]
            params += row_values

        self.execute_query(query, params)

    def update(self, new_data: dict):
        field_names = new_data.keys()
        placeholder_format = ', '.join([f'{field_name} = %s' for field_name in field_names])
        query = f"UPDATE {self.name_table} SET {placeholder_format}"
        params = list(new_data.values())

        self.execute_query(query, params)

    def delete(self):
        query = f"DELETE FROM {self.name_table} "

        self.execute_query(query)



# данные базы

DB_SETTINGS = {
    'host': 'localhost',
    'port': '5432',
    'database': 'fefu_back_2_3',
    'user': 'sail',
    'password': 'password'
}

Commands.database_settings = DB_SETTINGS


# структура базы

class BaseModel():
    table_name = ""
    commands = Commands()





# использование

# -----------------------------------------------------------------------------------------------------

BaseModel.commands.update_name_table("migrations")
print('До вставки:')
employes = BaseModel.commands.select("migration", "batch")
for item in employes:
    print(item)

# SQL: INSERT INTO migrations (migration, batch)
#  	   VALUES ('created_for_PavelMesenev', 1)

employees_data_in = [{"migration": "created_for_PavelMesenev", "batch" : "1"}]
BaseModel.commands.insert(rows=employees_data_in)

print('После вставки:')
employes = BaseModel.commands.select("migration", "batch")
for item in employes:
    print(item)

# -----------------------------------------------------------------------------------------------------

print('\nДо изменения')
BaseModel.commands.update_name_table("news") # меняем название таблицы с migrations на news

employes = BaseModel.commands.select("title", "slug")
for item in employes:
    print(item)

# SQL: UPDATE news SET title=FEFU;

BaseModel.commands.update(new_data={"title": "MP1st"})

print('После изменения')
employes = BaseModel.commands.select("title", "slug")
for item in employes:
    print(item)


# -----------------------------------------------------------------------------------------------------