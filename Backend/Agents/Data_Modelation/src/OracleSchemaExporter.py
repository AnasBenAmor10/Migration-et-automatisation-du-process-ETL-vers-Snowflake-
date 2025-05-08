import json
from datetime import datetime
import cx_Oracle
import os
from dotenv import load_dotenv
from utils.logger import logger

# Charger les variables d'environnement
load_dotenv()


# ! The object of this code is to extract all from oracle 11g database (Json file contain schema of the database)
class OracleSchemaExporter:
    def __init__(self):
        self.user = os.getenv("ORACLE_USER")
        self.password = os.getenv("ORACLE_PASSWORD")
        self.host = os.getenv("ORACLE_HOST")
        self.port = os.getenv("ORACLE_PORT")
        self.sid = os.getenv("ORACLE_SID")
        self.connection = None

    def connect(self):
        """Établit une connexion à la base de données Oracle"""
        try:
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.sid)
            logger.info(f"Trying to connect with service_name: {self.sid}...")
            self.connection = cx_Oracle.connect(
                user=self.user, password=self.password, dsn=dsn, encoding="UTF-8"
            )
            logger.info("Connexion établie avec succès.")
            return self.connection
        except cx_Oracle.DatabaseError as e:
            logger.warning(
                f"Service name approach failed, trying with SID... Error: {e}"
            )
            try:
                # Fall back to SID approach
                dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
                self.connection = cx_Oracle.connect(
                    user=self.user, password=self.password, dsn=dsn, encoding="UTF-8"
                )
                logger.info("Connexion établie avec succès (using SID).")
                return self.connection
            except cx_Oracle.DatabaseError as e:
                logger.error(f"Erreur lors de la connexion : {e}")
                raise

    def export_schema_to_json(self, schema_name, output_file=None):
        """
        Exporte les métadonnées du schéma vers un fichier JSON
        :param schema_name: Nom du schéma à exporter
        :param output_file: Chemin du fichier de sortie (optionnel)
        :return: Chemin du fichier généré
        """
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"oracle_schema_{schema_name}_{timestamp}.json"

        metadata = self.get_schema_metadata(schema_name)

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Export JSON réussi vers {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Erreur lors de l'export JSON : {e}")
            raise

    def get_schema_metadata(self, schema_name):
        """
        Récupère les métadonnées complètes du schéma spécifié
        :param schema_name: Nom du schéma à analyser
        :return: Dictionnaire contenant toutes les métadonnées du schéma
        """
        if not self.connection:
            logger.error("Aucune connexion active. Veuillez vous connecter d'abord.")
            return {}

        try:
            metadata = {
                "schema_name": schema_name,
                "extraction_date": datetime.now().isoformat(),
                "tables": self._get_tables_metadata(schema_name),
                # "views": self._get_views_metadata(schema_name),
                # "constraints": self._get_constraints_metadata(schema_name),
                "indexes": self._get_indexes_metadata(schema_name),
                "triggers": self._get_triggers_metadata(schema_name),
                # "sequences": self._get_sequences_metadata(schema_name),
                # "procedures": self._get_procedures_metadata(schema_name),
                # "functions": self._get_functions_metadata(schema_name),
                # "packages": self._get_packages_metadata(schema_name),
                "relationships": self._get_table_relationships(schema_name),
            }
            logger.info("Extraction des métadonnées du schéma terminée avec succès.")
            return metadata
        except cx_Oracle.DatabaseError as e:
            logger.error(f"Erreur lors de l'extraction des métadonnées : {e}")
            raise

    def _get_tables_metadata(self, schema_name):
        """Récupère les métadonnées des tables"""
        cursor = self.connection.cursor()
        try:
            # Requête pour obtenir les tables avec des infos supplémentaires
            cursor.execute(
                """
                SELECT 
                    table_name, 
                    tablespace_name,
                    num_rows,
                    avg_row_len,
                    blocks,
                    last_analyzed
                FROM all_tables 
                WHERE owner = :owner
                ORDER BY table_name
            """,
                {"owner": schema_name},
            )

            tables = {}
            for (
                table_name,
                tablespace,
                num_rows,
                avg_row_len,
                blocks,
                last_analyzed,
            ) in cursor:
                tables[table_name] = {
                    "tablespace": tablespace,
                    "columns": self._get_columns_metadata(schema_name, table_name),
                    "num_rows": num_rows,
                    "avg_row_len": avg_row_len,
                    "blocks": blocks,
                    "last_analyzed": last_analyzed,
                    "primary_key": self._get_primary_key(schema_name, table_name),
                    "foreign_keys": self._get_foreign_keys(schema_name, table_name),
                    "indexes": self._get_table_indexes(schema_name, table_name),
                    "comments": self._get_table_comments(schema_name, table_name),
                }
            return tables
        finally:
            cursor.close()

    def _get_columns_metadata(self, schema_name, table_name):
        """Récupère les métadonnées des colonnes d'une table"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    column_name, 
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    data_default,
                    char_length,
                    char_used
                FROM all_tab_columns 
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
            """,
                {"owner": schema_name, "table_name": table_name},
            )

            columns = []
            for col in cursor:
                column_info = {
                    "name": col[0],
                    "type": col[1],
                    "length": col[2],
                    "precision": col[3],
                    "scale": col[4],
                    "nullable": col[5] == "Y",
                    "default": col[6],
                    "char_length": col[7],
                    "char_used": col[8],
                    "comments": self._get_column_comment(
                        schema_name, table_name, col[0]
                    ),
                }
                columns.append(column_info)

            return columns
        finally:
            cursor.close()

    # def _get_views_metadata(self, schema_name):
    #     """Récupère les métadonnées des vues"""
    #     cursor = self.connection.cursor()
    #     try:
    #         cursor.execute(
    #             """
    #             SELECT
    #                 view_name,
    #                 text
    #             FROM all_views
    #             WHERE owner = :owner
    #             ORDER BY view_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         views = {}
    #         for view_name, text in cursor:
    #             views[view_name] = {
    #                 "definition": text,
    #                 "columns": self._get_view_columns(schema_name, view_name),
    #             }
    #         return views
    #     finally:
    #         cursor.close()

    def _get_view_columns(self, schema_name, view_name):
        """Récupère les colonnes d'une vue"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    column_name, 
                    data_type
                FROM all_tab_columns 
                WHERE owner = :owner AND table_name = :view_name
                ORDER BY column_id
            """,
                {"owner": schema_name, "view_name": view_name},
            )

            return [{"name": row[0], "type": row[1]} for row in cursor]
        finally:
            cursor.close()

    # def _get_constraints_metadata(self, schema_name):
    #     """Récupère les contraintes du schéma"""
    #     cursor = self.connection.cursor()
    #     try:
    #         # Contraintes de type CHECK
    #         cursor.execute(
    #             """
    #             SELECT
    #                 constraint_name,
    #                 table_name,
    #                 search_condition
    #             FROM all_constraints
    #             WHERE owner = :owner AND constraint_type = 'C'
    #             ORDER BY table_name, constraint_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         check_constraints = {}
    #         for name, table, condition in cursor:
    #             if table not in check_constraints:
    #                 check_constraints[table] = []
    #             check_constraints[table].append({"name": name, "condition": condition})

    #         # Contraintes UNIQUE
    #         cursor.execute(
    #             """
    #             SELECT
    #                 c.constraint_name,
    #                 c.table_name,
    #                 cc.column_name
    #             FROM all_constraints c
    #             JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
    #             WHERE c.owner = :owner AND c.constraint_type = 'U'
    #             ORDER BY c.table_name, c.constraint_name, cc.position
    #         """,
    #             {"owner": schema_name},
    #         )

    #         unique_constraints = {}
    #         for name, table, column in cursor:
    #             if table not in unique_constraints:
    #                 unique_constraints[table] = {}
    #             if name not in unique_constraints[table]:
    #                 unique_constraints[table][name] = []
    #             unique_constraints[table][name].append(column)

    #         return {"check": check_constraints, "unique": unique_constraints}
    #     finally:
    #         cursor.close()

    def _get_indexes_metadata(self, schema_name):
        """Récupère les index du schéma"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    i.index_name,
                    i.table_name,
                    i.uniqueness,
                    i.index_type,
                    ic.column_name,
                    ic.descend,
                    i.partitioned
                FROM all_indexes i
                JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
                WHERE i.owner = :owner
                ORDER BY i.table_name, i.index_name, ic.column_position
            """,
                {"owner": schema_name},
            )

            indexes = {}
            for name, table, uniqueness, type, column, descend, partitioned in cursor:
                if table not in indexes:
                    indexes[table] = {}
                if name not in indexes[table]:
                    indexes[table][name] = {
                        "unique": uniqueness == "UNIQUE",
                        "type": type,
                        "partitioned": partitioned == "YES",
                        "columns": [],
                    }
                indexes[table][name]["columns"].append(
                    {"name": column, "order": descend}
                )

            return indexes
        finally:
            cursor.close()

    def _get_triggers_metadata(self, schema_name):
        """Récupère les triggers du schéma"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    trigger_name,
                    table_name,
                    trigger_type,
                    triggering_event,
                    status,
                    trigger_body
                FROM all_triggers
                WHERE owner = :owner
                ORDER BY table_name, trigger_name
            """,
                {"owner": schema_name},
            )

            triggers = {}
            for name, table, type, event, status, body in cursor:
                if table not in triggers:
                    triggers[table] = []
                triggers[table].append(
                    {
                        "name": name,
                        "type": type,
                        "event": event,
                        "status": status,
                        "body": body,
                    }
                )

            return triggers
        finally:
            cursor.close()

    # def _get_sequences_metadata(self, schema_name):
    #     """Récupère les séquences du schéma"""
    #     cursor = self.connection.cursor()
    #     try:
    #         cursor.execute(
    #             """
    #             SELECT
    #                 sequence_name,
    #                 min_value,
    #                 max_value,
    #                 increment_by,
    #                 cycle_flag,
    #                 order_flag,
    #                 cache_size,
    #                 last_number
    #             FROM all_sequences
    #             WHERE sequence_owner = :owner
    #             ORDER BY sequence_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         sequences = {}
    #         for (
    #             name,
    #             min_val,
    #             max_val,
    #             increment,
    #             cycle,
    #             order,
    #             cache,
    #             last_num,
    #         ) in cursor:
    #             sequences[name] = {
    #                 "min_value": min_val,
    #                 "max_value": max_val,
    #                 "increment": increment,
    #                 "cycles": cycle == "Y",
    #                 "ordered": order == "Y",
    #                 "cache_size": cache,
    #                 "last_number": last_num,
    #             }

    #         return sequences
    #     finally:
    #         cursor.close()

    # def _get_procedures_metadata(self, schema_name):
    #     """Récupère les procédures stockées du schéma"""
    #     cursor = self.connection.cursor()
    #     try:
    #         cursor.execute(
    #             """
    #             SELECT
    #                 object_name,
    #                 status,
    #                 created,
    #                 last_ddl_time
    #             FROM all_objects
    #             WHERE owner = :owner AND object_type = 'PROCEDURE'
    #             ORDER BY object_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         procedures = {}
    #         for name, status, created, last_ddl in cursor:
    #             procedures[name] = {
    #                 "status": status,
    #                 "created": created,
    #                 "last_modified": last_ddl,
    #                 "source": self._get_source_code(schema_name, "PROCEDURE", name),
    #             }

    #         return procedures
    #     finally:
    #         cursor.close()

    # def _get_functions_metadata(self, schema_name):
    #     """Récupère les fonctions du schéma"""
    #     cursor = self.connection.cursor()
    #     try:
    #         cursor.execute(
    #             """
    #             SELECT
    #                 object_name,
    #                 status,
    #                 created,
    #                 last_ddl_time
    #             FROM all_objects
    #             WHERE owner = :owner AND object_type = 'FUNCTION'
    #             ORDER BY object_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         functions = {}
    #         for name, status, created, last_ddl in cursor:
    #             functions[name] = {
    #                 "status": status,
    #                 "created": created,
    #                 "last_modified": last_ddl,
    #                 "source": self._get_source_code(schema_name, "FUNCTION", name),
    #             }

    #         return functions
    #     finally:
    #         cursor.close()

    # def _get_packages_metadata(self, schema_name):
    #     """Récupère les packages du schéma"""
    #     cursor = self.connection.cursor()
    #     try:
    #         cursor.execute(
    #             """
    #             SELECT
    #                 object_name,
    #                 status,
    #                 created,
    #                 last_ddl_time
    #             FROM all_objects
    #             WHERE owner = :owner AND object_type = 'PACKAGE'
    #             ORDER BY object_name
    #         """,
    #             {"owner": schema_name},
    #         )

    #         packages = {}
    #         for name, status, created, last_ddl in cursor:
    #             packages[name] = {
    #                 "status": status,
    #                 "created": created,
    #                 "last_modified": last_ddl,
    #                 "spec": self._get_source_code(schema_name, "PACKAGE", name),
    #                 "body": self._get_source_code(schema_name, "PACKAGE BODY", name),
    #             }

    #         return packages
    #     finally:
    #         cursor.close()

    def _get_source_code(self, schema_name, object_type, object_name):
        """Récupère le code source d'un objet de base de données"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT text
                FROM all_source
                WHERE owner = :owner AND name = :name AND type = :type
                ORDER BY line
            """,
                {"owner": schema_name, "name": object_name, "type": object_type},
            )

            return "\n".join([row[0] for row in cursor]) or None
        except cx_Oracle.DatabaseError:
            return None
        finally:
            cursor.close()

    def _get_primary_key(self, schema_name, table_name):
        """Récupère la clé primaire d'une table"""
        cursor = self.connection.cursor()
        try:
            # Récupérer le nom de la contrainte PK
            cursor.execute(
                """
                SELECT 
                    constraint_name
                FROM all_constraints
                WHERE owner = :owner AND table_name = :table_name AND constraint_type = 'P'
            """,
                {"owner": schema_name, "table_name": table_name},
            )

            pk_constraint = cursor.fetchone()
            if not pk_constraint:
                return None

            # Récupérer les colonnes de la PK
            cursor.execute(
                """
                SELECT 
                    column_name,
                    position
                FROM all_cons_columns
                WHERE owner = :owner AND constraint_name = :constraint_name
                ORDER BY position
            """,
                {"owner": schema_name, "constraint_name": pk_constraint[0]},
            )

            columns = [row[0] for row in cursor]
            return {"constraint_name": pk_constraint[0], "columns": columns}
        finally:
            cursor.close()

    def _get_foreign_keys(self, schema_name, table_name):
        """Récupère les clés étrangères d'une table"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    a.constraint_name,
                    a.r_owner,
                    a.r_constraint_name,
                    b.table_name as r_table_name,
                    c.column_name as column_name,
                    d.column_name as r_column_name,
                    c.position
                FROM all_constraints a
                JOIN all_constraints b ON a.r_owner = b.owner AND a.r_constraint_name = b.constraint_name
                JOIN all_cons_columns c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
                JOIN all_cons_columns d ON b.owner = d.owner AND b.constraint_name = d.constraint_name AND c.position = d.position
                WHERE a.owner = :owner AND a.table_name = :table_name AND a.constraint_type = 'R'
                ORDER BY a.constraint_name, c.position
            """,
                {"owner": schema_name, "table_name": table_name},
            )

            fks = {}
            for name, r_owner, r_constraint, r_table, column, r_column, pos in cursor:
                if name not in fks:
                    fks[name] = {
                        "constraint_name": name,
                        "referenced_schema": r_owner,
                        "referenced_table": r_table,
                        "referenced_constraint": r_constraint,
                        "columns": [],
                        "referenced_columns": [],
                    }
                fks[name]["columns"].append(column)
                fks[name]["referenced_columns"].append(r_column)

            return list(fks.values())
        finally:
            cursor.close()

    def _get_table_indexes(self, schema_name, table_name):
        """Récupère les index d'une table spécifique"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    i.index_name,
                    i.uniqueness,
                    i.index_type,
                    ic.column_name,
                    ic.descend,
                    i.partitioned
                FROM all_indexes i
                JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
                WHERE i.owner = :owner AND i.table_name = :table_name
                ORDER BY i.index_name, ic.column_position
            """,
                {"owner": schema_name, "table_name": table_name},
            )

            indexes = {}
            for name, uniqueness, type, column, descend, partitioned in cursor:
                if name not in indexes:
                    indexes[name] = {
                        "unique": uniqueness == "UNIQUE",
                        "type": type,
                        "partitioned": partitioned == "YES",
                        "columns": [],
                    }
                indexes[name]["columns"].append({"name": column, "order": descend})

            return list(indexes.values())
        finally:
            cursor.close()

    def _get_table_comments(self, schema_name, table_name):
        """Récupère le commentaire d'une table"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT comments
                FROM all_tab_comments
                WHERE owner = :owner AND table_name = :table_name
            """,
                {"owner": schema_name, "table_name": table_name},
            )

            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()

    def _get_column_comment(self, schema_name, table_name, column_name):
        """Récupère le commentaire d'une colonne"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT comments
                FROM all_col_comments
                WHERE owner = :owner AND table_name = :table_name AND column_name = :column_name
            """,
                {
                    "owner": schema_name,
                    "table_name": table_name,
                    "column_name": column_name,
                },
            )

            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()

    def _get_table_relationships(self, schema_name):
        """Récupère les relations entre tables (clés étrangères)"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT 
                    a.table_name as source_table,
                    a.constraint_name as fk_name,
                    c_pk.table_name as target_table,
                    LISTAGG(c_fk.column_name, ',') WITHIN GROUP (ORDER BY c_fk.position) as fk_columns,
                    LISTAGG(c_pk.column_name, ',') WITHIN GROUP (ORDER BY c_fk.position) as pk_columns
                FROM all_constraints a
                JOIN all_constraints b ON a.r_owner = b.owner AND a.r_constraint_name = b.constraint_name
                JOIN all_cons_columns c_fk ON a.owner = c_fk.owner AND a.constraint_name = c_fk.constraint_name
                JOIN all_cons_columns c_pk ON b.owner = c_pk.owner AND b.constraint_name = c_pk.constraint_name AND c_fk.position = c_pk.position
                WHERE a.owner = :owner AND a.constraint_type = 'R'
                GROUP BY a.table_name, a.constraint_name, c_pk.table_name
                ORDER BY a.table_name, a.constraint_name
            """,
                {"owner": schema_name},
            )

            return [
                {
                    "source_table": row[0],
                    "fk_name": row[1],
                    "target_table": row[2],
                    "fk_columns": row[3].split(","),
                    "pk_columns": row[4].split(","),
                }
                for row in cursor
            ]
        finally:
            cursor.close()

    def close_connection(self):
        """Ferme la connexion à la base de données"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Connexion fermée.")
