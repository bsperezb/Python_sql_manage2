import os
import logging
from db.database import Database
from excel.excel_handler import ExcelHandler
from utils.general import find_matching_tuple, clean_string
from utils.graph import plot_histogram_and_std, calcular_intervalo_confianza, bootstrap_ci
from dotenv import load_dotenv

# Configuración básica del registro (logging)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno desde el archivo .env
load_dotenv()


def main():
    # Configurar la conexión a la base de datos
    words_to_remove = ['VDA', 'VEREDA', 'CORREGIMIENTO', 'CORREG', '']
    id_tipo_institucion = 10 # institucion educativa
    array_vereda_error = []
    array_reliability = []
    
    # Array items
    item_dane_errors = []
    item_excel_error_reiability = []
    item_excel_error_reiability_86_90 = []
    item_not_zone = []

    db = Database(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    # Registrar la conexión exitosa a la base de datos
    logger.info("Conexión a la base de datos establecida exitosamente.")

    # Preguntar antes de leer el archivo Excel
    user_input = input("¿Deseas leer el archivo Excel? (y/n): ")
    if user_input.lower() == 'y':
        # Leer datos desde el archivo Excel
        excel_handler = ExcelHandler("sedes_verificadas_limpias_segunda_revision.xlsx")
        data = excel_handler.read_excel()
        count = 0
        count_dane_error = 0
        count_excel_error_reiability = 0
        count_excel_error_reiability_86_90 = 0
        count_not_zone = 0
        count_existing_data = 0

        count_veredas_exitosas = 0
        count_veredas_exitosas_rural = 0
        count_veredas_exitosas_urbana = 0
        for item in data:
            # import ipdb; ipdb.set_trace()
            #Get DATA
            dane_code = item['codigo_municiopio']
            nombre_municipio = item['nombre_municipio']
            codigo_institucion = item['codigo_establecimiento_educativo']
            nombre_institucion = item['nombre_establecimiento_educativo']
            codigo_sede = item['codigo_sede']
            nombre_sede = item['nombre_sede']
            zona_sede= item['zona_sede']
            vereda_direccion = item['direccion']
            db_institucion = db.get_institucion(codigo_institucion)
            db_institucio_sede = db.get_institucion(codigo_sede)

            count = count + 1
            print('count:')
            print(count)

            # VALIDATE DATA INTEGRITY--------------------------------------------
            # Continua si existe ya registrada la institucion:
            # import ipdb; ipdb.set_trace()
            if db_institucion or db_institucio_sede:
                # import ipdb; ipdb.set_trace()
                print(db_institucion)
                print(db_institucio_sede)
                id_institucion_db = '' 
                if db_institucion:
                    id_institucion_db = db_institucion[0][0] 
                if db_institucio_sede:
                    id_institucion_db = db_institucio_sede[0][0] 

                logger.info(f"Institucion Ya existe en la DB ,Nombre Sede: {nombre_sede}, id istitucion: {id_institucion_db} ")
                count_existing_data = count_existing_data + 1
                continue

            # get lista de veredas, TODO manejar errores
            if not dane_code: 
                logger.info("No existe codigo dane")
                continue
            lista_veredas = db.get_veredas_by_municipio(dane_code)
            if not lista_veredas: 
                item_dane_errors.append(item)
                count_dane_error += 1
                logger.info(f"NO  se encontro la lista para el codigo dane, dane code {dane_code}")
                continue
            # TODO Add nombre vereda
            # verenda_name = 'PORTUGAL'
            # CHECK DATA -------------------------------------------
            vereda_name = vereda_direccion
            if zona_sede == 'RURAL':
                vereda_name_clean = clean_string(vereda_name, words_to_remove) 
                vereda_match, porcentaje_fiabilidad = find_matching_tuple(lista_veredas, vereda_name_clean)
                vereda_match2, porcentaje_fiabilidad2 = find_matching_tuple(lista_veredas, 'VEREDA ' + vereda_name_clean )
                if(porcentaje_fiabilidad2 > porcentaje_fiabilidad):
                    vereda_match = vereda_match2
                    porcentaje_fiabilidad = porcentaje_fiabilidad2
                if not vereda_match: 
                    logger.info(f"No hay resultado para el match de la vereda en rural vereda: {vereda_name}, municipio id: {dane_code}")
                    continue

                print('Fiabilidad: ', porcentaje_fiabilidad, '-------------')
                print('RURAL')
                print('Vereda Name: ', vereda_name)
                print('Vereda Name clean: ', vereda_name_clean)
                print('Vereda match: ', vereda_match)
                # VALIDATE RELIABILITY
                # import ipdb; ipdb.set_trace()
                array_reliability.append(porcentaje_fiabilidad)
                if  porcentaje_fiabilidad < 86 : 
                    array_vereda_error.append(vereda_name)
                    item_excel_error_reiability_86_90.append(item)
                    count_excel_error_reiability += 1
                    logger.info(f"Fiavilidad menro al 86% vereda: {vereda_name}, municipio id: {dane_code}")
                    continue


                if  porcentaje_fiabilidad < 90: 
                    array_vereda_error.append(vereda_name)
                    item_excel_error_reiability.append(item)
                    count_excel_error_reiability_86_90 += 1
                    logger.info(f"Fiavilidad menro al 96% vereda: {vereda_name}, municipio id: {dane_code}")
                    continue

                # TODO insertar institucion ----------------------------------------
                # import ipdb; ipdb.set_trace()
                vereda_id = vereda_match[0]
                municipio_id = vereda_match[2]
                print('Municipio id: ', municipio_id)
                print('Vereda id: ', vereda_id)
                print('Nombre sede: ', nombre_sede)
                # def insert_institucion(self, CodDaneInstitucion, NombreInstitucion, idTipoInstitucion, idMcpio, idVereda):
                db.insert_institucion(codigo_sede, nombre_sede, id_tipo_institucion, municipio_id, vereda_id   )
                count_veredas_exitosas = count_veredas_exitosas + 1
                count_veredas_exitosas_rural = count_veredas_exitosas_rural + 1

            elif zona_sede == 'URBANA':
                # Obtener campo llamado cabecera municipal en las veredas 
                vereda_match, porcentaje_fiabilidad = find_matching_tuple(lista_veredas, 'CABECERA MUNICIPAL')
                # import ipdb; ipdb.set_trace()
                vereda_id = vereda_match[0]
                municipio_id = vereda_match[2]

                print('Fiabilidad: Urbana', '-------------')
                print('URBANA ')
                print('Direccion: ', vereda_name)
                print('Municipio id: ', municipio_id)
                print('Vereda id: ', vereda_id)
                print('Nombre sede: ', nombre_sede)
                db.insert_institucion(codigo_sede, nombre_sede, id_tipo_institucion, municipio_id, vereda_id   )
                count_veredas_exitosas = count_veredas_exitosas + 1
                count_veredas_exitosas_urbana = count_veredas_exitosas_urbana + 1

            else :
                # Agregar a array de excel que vamos a devolver para que revisen
                item_not_zone.append(item)
                count_not_zone += 1
                continue

        # user_input = input("¿Deseas ? (y/n): ")
        # if user_input.lower() != 'y':
        #     return
        # 
        # Preguntar antes de insertar datos en la tabla de instituciones

        # user_input_insert = input("¿Deseas insertar los datos en la tabla de instituciones? (y/n): ")
        # if user_input_insert.lower() == 'y':
        #     # Insertar datos en la tabla de instituciones
        #     db.insert_data(data)
        #     logger.info("Datos insertados en la tabla de instituciones.")
        # else:
        #     logger.info("Operación cancelada. No se han insertado datos.")
    else:
        logger.info("Operación cancelada. No se ha leído el archivo Excel.")

    # print('Los siguientes Codigos no estan en la DB')
    # print(array_dane_errors)
    # item_dane_errors = []
    # item_excel_error_reiability = []
    # item_not_zone = []

    excel_handler = ExcelHandler("sedesEducativasAntioquiaRural.xlsx")

    print('CONTADORES --------------')
    print('Contador total', count)
    print('Contador Veredas registradas', count_veredas_exitosas)
    print('Contador Veredas registradas Rural', count_veredas_exitosas_rural)
    print('Contador Veredas registradas urbanas', count_veredas_exitosas_urbana)
    print('Contador ya existentes en base de datos', count_existing_data)

    print('Contador erro codigo dane municipio', count_dane_error)
    print('Contador error en confiabilidad excel', count_excel_error_reiability)
    print('Contador error en confiabilidad excel 86-90', count_excel_error_reiability_86_90)
    print('Contador Zona horaria desconocida', count_not_zone)
    excel_handler.generate_csv('item_dane_errors', item_dane_errors)
    excel_handler.generate_csv('item_excel_error_reiability_86_90', item_excel_error_reiability_86_90)
    excel_handler.generate_csv('item_excel_error_reiability', item_excel_error_reiability)
    excel_handler.generate_csv('item_not_zone', item_not_zone)

    logger.info("Excels generados")

    calcular_intervalo_confianza(array_reliability)
    bootstrap_ci(array_reliability)
    plot_histogram_and_std(array_reliability)

def create_local_government_header():
    db = Database(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    # Get municipios
    count_registros = 0
    municipios = db.get_municipios()

    # Loop by Municipio
    vereda_request = 'CABECERA MUNICIPAL'
    for municipio in municipios:
        vereda_in_municipio = False

        municipio_dane_code = municipio[1]
        municipio_id = municipio[0]
        # print(municipio_id)
        veredas = db.get_veredas_by_municipio(municipio_dane_code)
        # Check if Cabecera Municipal is in Municipio
        for vereda in veredas:
            # print(vereda)
            vereda_name = vereda[1]
            if vereda_name ==  vereda_request:
                vereda_in_municipio = True
                # print(vereda)

        # Add Vereda if Cabecera Municipal not in Municipio
        if not vereda_in_municipio :
            db.insert_vereda(str(municipio_id), vereda_request)
            count_registros = count_registros + 1
        # else: 
        #     print('en el else')
    if count_registros > 0 :
        logger.info("Campo Cabecera Municipal Guardado Exitosamente.")
        logger.info(f"Numero de campos alterados: {count_registros}")
    else:
        logger.info("Todos los Municipios ya cuentan con cabeceras municipales agregadas.")

    return  

if __name__ == "__main__":
    create_local_government_header()
    main()
