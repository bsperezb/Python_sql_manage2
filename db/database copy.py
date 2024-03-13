import mysql.connector
import logging
import os

class Database:
    def __init__(self, host, port, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

# Resto del código de la clase...

    def get_municipios(self):
        try:
            # Consulta SQL para obtener todos los datos de la tabla "instituciones"
            query = "SELECT * FROM municipio"
            self.cursor.execute(query)

            # Recupera todos los resultados como una lista de diccionarios
            result = self.cursor.fetchall()

            # Imprime los resultados (esto es opcional, puedes quitarlo en producción)
            # logging.info("Resultados de la consulta a la tabla 'instituciones':")
            # for row in result:
            #     logging.info(row)

            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None

    def get_instituciones(self):
        try:
            # Consulta SQL para obtener todos los datos de la tabla "instituciones"
            query = "SELECT * FROM institucion"
            self.cursor.execute(query)

            # Recupera todos los resultados como una lista de diccionarios
            result = self.cursor.fetchall()

            # Imprime los resultados (esto es opcional, puedes quitarlo en producción)
            # logging.info("Resultados de la consulta a la tabla 'instituciones':")
            # for row in result:
            #     logging.info(row)

            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None

    def get_institucion(self, codigo_dane_institucion):
        try:
            # Consulta SQL para obtener todos los datos de la tabla "instituciones"
            query = f"SELECT * FROM institucion WHERE CodDaneInstitucion = {codigo_dane_institucion}"
            self.cursor.execute(query)

            # Recupera todos los resultados como una lista de diccionarios
            result = self.cursor.fetchall()

            # Imprime los resultados (esto es opcional, puedes quitarlo en producción)
            # logging.info("Resultados de la consulta a la tabla 'instituciones':")
            # for row in result:
            #     logging.info(row)

            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None



    def get_instituciones_by_vereda(self, id):
        try:
            # Consulta SQL para obtener todos los datos de la tabla "instituciones"
            query = f"SELECT * FROM `institucion` WHERE idVereda = {id}"
            self.cursor.execute(query)

            # Recupera todos los resultados como una lista de diccionarios
            result = self.cursor.fetchall()

            # Imprime los resultados (esto es opcional, puedes quitarlo en producción)
            # logging.info("Resultados de la consulta a la tabla 'instituciones':")
            # for row in result:
            #     logging.info(row)

            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None

    def get_veredas_by_municipio(self, id_dane_municipio):
        try:
            # Consulta SQL para obtener todos los datos de la tabla "vereda" con el ID de municipio dado
            # query = f"SELECT * FROM vereda WHERE Municipio_idDaneMunicipio = {id_dane_municipio}"
            query = (
                f"SELECT v.* "
                f"FROM vereda v "
                f"JOIN municipio m ON v.Municipio_idMunicipio = m.idMunicipio "
                f"WHERE m.idDaneMunicipio = {id_dane_municipio};"
            )
            self.cursor.execute(query)

            # Recupera todos los resultados como una lista de diccionarios
            result = self.cursor.fetchall()

            # Imprime los resultados (esto es opcional, puedes quitarlo en producción)
            # logging.info(f"Resultados de la consulta a la tabla 'vereda' para el municipio con ID {id_dane_municipio}:")
            # for row in result:
            #     logging.info(row)

            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None

    def insert_institucion(self, CodDaneInstitucion, NombreInstitucion, idTipoInstitucion, idMcpio, idVereda):
        try:
            # Consulta SQL para insertar datos en la tabla "institucion"
            query = (
                f"INSERT INTO sigla.institucion "
                f"(CodDaneInstitucion, NombreInstitucion, idTipoInstitucion, idMcpio, idVereda) "
                f"VALUES('{CodDaneInstitucion}', '{NombreInstitucion}', {idTipoInstitucion}, {idMcpio}, {idVereda})"
            )
            self.cursor.execute(query)

            # Confirmar la transacción
            self.connection.commit()

            logging.info("Datos insertados en la tabla 'institucion'.")
            return True

        except mysql.connector.Error as err:
            self.connection.rollback()
            logging.error(f"Error al ejecutar la consulta: {err}")
            return False

    def insert_vereda(self, municipio_id, nombre_vereda):
        """
        Inserta una nueva vereda en la base de datos.

        :param municipio_id: El número DANE del municipio al que pertenece la vereda.
        :type municipio_id: str
        :param nombre_vereda: El nombre de la vereda.
        :type nombre_vereda: str
        :return: True si la inserción fue exitosa, False en caso de error.
        :rtype: bool
        """
        try:
            # Consulta SQL para insertar datos en la tabla "vereda"
            query = (
                f"INSERT INTO sigla.vereda "
                f"(Municipio_idMunicipio, NombreVereda) "
                f"VALUES('{municipio_id}', '{nombre_vereda}')"
            )
            self.cursor.execute(query)

            # Confirmar la transacción
            self.connection.commit()

            logging.info(f"Datos insertados en la tabla 'vereda', Municipio ->  {municipio_id}")
            return True

        except mysql.connector.Error as err:
            self.connection.rollback()
            logging.error(f"Error al ejecutar la consulta: {err}")
            return False
