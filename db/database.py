
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

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result

        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            return None

    def execute_insert_query(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
            logging.info("Operación de inserción exitosa.")
            return True

        except mysql.connector.Error as err:
            self.connection.rollback()
            logging.error(f"Error al ejecutar la consulta de inserción: {err}")
            return False

    def get_municipios(self):
        query = "SELECT * FROM municipio"
        return self.execute_query(query)

    def get_instituciones(self):
        query = "SELECT * FROM institucion"
        return self.execute_query(query)

    def get_institucion(self, codigo_dane_institucion):
        query = f"SELECT * FROM institucion WHERE CodDaneInstitucion = {codigo_dane_institucion}"
        return self.execute_query(query)

    def get_instituciones_by_vereda(self, id):
        query = f"SELECT * FROM `institucion` WHERE idVereda = {id}"
        return self.execute_query(query)

    def get_veredas_by_municipio(self, id_dane_municipio):
        query = (
            f"SELECT v.* "
            f"FROM vereda v "
            f"JOIN municipio m ON v.Municipio_idMunicipio = m.idMunicipio "
            f"WHERE m.idDaneMunicipio = {id_dane_municipio};"
        )
        return self.execute_query(query)

    def insert_institucion(self, CodDaneInstitucion, NombreInstitucion, idTipoInstitucion, idMcpio, idVereda):
        query = (
            f"INSERT INTO sigla.institucion "
            f"(CodDaneInstitucion, NombreInstitucion, idTipoInstitucion, idMcpio, idVereda) "
            f"VALUES('{CodDaneInstitucion}', '{NombreInstitucion}', {idTipoInstitucion}, {idMcpio}, {idVereda})"
        )
        return self.execute_insert_query(query)

    def insert_vereda(self, municipio_id, nombre_vereda):
        query = (
            f"INSERT INTO sigla.vereda "
            f"(Municipio_idMunicipio, NombreVereda) "
            f"VALUES('{municipio_id}', '{nombre_vereda}')"
        )
        return self.execute_insert_query(query)
