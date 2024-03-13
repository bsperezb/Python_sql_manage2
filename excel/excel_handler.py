import pandas as pd

class ExcelHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_excel(self):
        # Utiliza pandas para leer el archivo Excel
        df = pd.read_excel(self.file_path)

        # Imprime los datos
        # print("Datos del archivo Excel:")
        # print(df)
        # print("Encabezados")
        # print(df.columns)

        # Itera sobre las filas y almacena cada registro en una lista
        data_list = []
        for index, row in df.iterrows():
            # import ipdb; ipdb.set_trace()
            record = {
                "codigo_municiopio": row["Código Municipio"],
                "nombre_municipio": row[" Nombre municipio"],
                "codigo_establecimiento_educativo": row[" Código Establecimiento Educativo"],
                "nombre_establecimiento_educativo": row[" Nombre Establecimiento Educativo"],
                "codigo_sede": row[" Código Sede"],
                "nombre_sede": row[" Nombre Sede"],
                "zona_sede": row["Zona Sede"],
                "direccion": row["Dirección"],
                "estado_sede": row["Estado Sede"],
                # Agrega más columnas según la estructura de tu archivo Excel
            }
            data_list.append(record)

        # Imprime la lista de datos
        # print("Lista de datos:")
        # print(data_list)

        return data_list

    def generate_csv2(self, file_name, data_array):
        # Crea el nombre completo del archivo CSV en la misma ubicación que el archivo Excel
        csv_file_path = f"{self.file_path[:-5]}_{file_name}.csv"

        # Escribe los datos en el archivo CSV
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            # Configura el escritor CSV
            csv_writer = csv.DictWriter(csv_file, fieldnames=data_array[0].keys())

            # Escribe los encabezados
            csv_writer.writeheader()

            # Escribe los datos
            csv_writer.writerows(data_array)

        print(f"Archivo CSV '{file_name}' generado exitosamente en: {csv_file_path}")

    def generate_csv(self, file_name, data_array):
            # Crea el nombre completo del archivo CSV en la misma ubicación que el archivo Excel
            csv_file_path = f"{self.file_path[:-5]}_{file_name}.csv"

            # Crea un DataFrame de Pandas a partir de los datos
            df = pd.DataFrame(data_array)

            # Guarda el DataFrame en un archivo CSV
            df.to_csv(csv_file_path, index=False, encoding='utf-8')

            print(f"Archivo CSV '{file_name}' generado exitosamente en: {csv_file_path}")
