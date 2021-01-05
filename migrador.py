# encoding: latin-1
import cx_Oracle
import fdb
import psycopg2
import sys
import datetime
import time
import csv
import os

"""
        MIGRADOR GERAL PARA ORACLE E FIREBIRD PARA POSTGRESQL
        PARA USAR BASTAR TER O BANCO DE ORIGEM RESTAURADO E SUAS CONFIGURACOES COLOCADAS CORRETAMENTE
                             O BANCO DE DESTINO CRIADO E SUAS CONFIGURACOES COLOCADAS CORRETAMENTE

        CASO SEJA NECESSARIO MIGRAR APENAS UMA TABELA PODE-SE PASSAR COMO ARGUMENTO: python migrador.py TABELA

        AO FINAL DO PROCESSO O MIGRADOR RETORNARÁ ERROS CASO OCORRAM POR TABELA

        USA A ENGENHARIA DE COPY (GRAVA UM ARQUIVO), POIS SUA EFICIENCIA E MELHOR QUE O INSERT

        NO FINAL DO SCRIPT TEM COMO SE CONFIGURA AS INFORMACOES DE BANCOS DE DADOS DE ORIGEM E DESTINO

"""


class migrador_firebird:
    def __init__(self, tabela):
        # variaveis conexao banco de dados origem
        self.host_origem = 'localhost'
        self.database_origem = 'C:/Users/dabes/Downloads/DBORIZANIA.FDB'
        self.user_origem = 'SYSDBA'
        self.senha_origem = 'masterkey'
        # variaves conexoes banco de dados destino
        self.user_destino = "postgres"
        self.senha_destino = "postgres"
        self.host_destino = "10.0.18.70"
        self.database_destino = "teste_perf_mem2"
        self.porta_destino = 5433
        self.erros = []
        self.current = 1
        self.offset = 10000
        self.tabela = tabela
        if tabela != None:
            self.filtro = " and rdb$relation_name = '%s' " % tabela
        else:
            self.filtro = ""

    def connect(self):
        """ CONECTA AOS BANCOS DE ENTRADA E SAIDA"""
        self.con_origem = fdb.connect(
            host=self.host_origem, database=self.database_origem,
            user=self.user_origem, password=self.senha_origem
        )

        self.con_destino = psycopg2.connect(
            "user=%s password=%s host=%s port=%s dbname=%s" % (self.user_destino, self.senha_destino, self.host_destino, self.porta_destino, self.database_destino))

        self.cur_origem = self.con_origem.cursor()
        self.cur_destino = self.con_destino.cursor()

        self.con_destino.autocommit = True

    def migrar(self):
        """  COPIA A(S) TABELA(S) PARA O BANCO DE SAIDA """
        start = time.time()
        if not os.path.exists('output'):
            os.makedirs('output')
        # seleciona as tabelas
        res = self.cur_origem.execute(
            "select rdb$relation_name from rdb$relations where rdb$view_blr is null and (rdb$system_flag is null or rdb$system_flag = 0) %s;" % self.filtro)

        # para cada tabela
        for row, in res.fetchall():
            row = row.strip()

            # conta os registros
            countsql = self.cur_origem.execute(
                "select count(*) as total from %s " % row)
            count, = countsql.fetchall()[0]
            start_time = time.time()
            start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print "MIGRANDO: %s\n          NRO REGISTROS: %s registros\n          INICIO: %s" % (
                row, count, start_datetime)

            # gera o create table e trunca a tabela ( se ja existir )
            create, tipos = self.ddl_table(row)
            self.cur_destino.execute(create)
            self.cur_destino.execute("TRUNCATE TABLE %s" % row)

            # busca os dados
            self.cur_origem.execute("select * from %s " % (row))

            # grava os dados no TXT
            with open("output/%s.txt" % row, "wb") as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerows(self.cur_origem.fetchall())

            # le o arquivo gravado e copia para o banco destino
            with open("output/%s.txt" % row, "r") as f:
                try:
                    self.cur_destino.copy_expert(
                        """COPY %s FROM STDIN WITH QUOTE '"' DELIMITER '|' NULL '' CSV """ % row, f)
                except Exception as e:
                    self.erros.append(["%s" % row, e])
                    end_time = time.time()
                    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("          FIM: %s\n          TEMPO: %ss\n          TABELA COM ERRO %s" %
                          (end_datetime, round(end_time-start_time, 0), e))
                else:
                    end_time = time.time()
                    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("          FIM: %s\n          TEMPO: %ss\n          OK" %
                          (end_datetime, round(end_time-start_time, 0)))
        end = time.time()
        print("TEMPO GASTO: %s s" % (end-start))

    def printa_erros(self):
        """ retorna os erros encontrados durante a migracao """
        for cada in self.erros:
            print "%s\t%s\n--------------------------------------\n" % (
                cada[0], cada[1])

    def ddl_table(self, tabela):
        """ gera o ddl (create table e tipos das colunas)"""
        sql = """SELECT
            RF.RDB$FIELD_NAME FIELD_NAME,
            CASE F.RDB$FIELD_TYPE
                WHEN 7 THEN
                CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'INT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                END
                WHEN 8 THEN
                CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'INTEGER'
                    WHEN 1 THEN 'NUMERIC('  || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                END
                WHEN 9 THEN 'QUAD'
                WHEN 10 THEN 'FLOAT'
                WHEN 12 THEN 'DATE'
                WHEN 13 THEN 'TIME'
                WHEN 14 THEN 'CHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / COALESCE(CH.RDB$BYTES_PER_CHARACTER,1))) || ') '
                WHEN 16 THEN
                CASE F.RDB$FIELD_SUB_TYPE
                    WHEN 0 THEN 'BIGINT'
                    WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                    WHEN 2 THEN 'DECIMAL'
                END
                WHEN 27 THEN 'NUMERIC'
                WHEN 35 THEN 'TIMESTAMP'
                WHEN 37 THEN 'VARCHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / COALESCE(CH.RDB$BYTES_PER_CHARACTER,1))) || ')'
                WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / COALESCE(CH.RDB$BYTES_PER_CHARACTER,1))) || ')'
                WHEN 45 THEN 'BLOB_ID'
                WHEN 261 THEN 'TEXT'
                ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'
            END FIELD_TYPE
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)
            LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
            LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = F.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))
            WHERE (RF.RDB$RELATION_NAME = '%s') AND (COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0)
            ORDER BY RF.RDB$FIELD_POSITION;""" % (tabela)
        res = self.cur_origem.execute(sql)
        table = "CREATE TABLE IF NOT EXISTS %s (" % tabela
        tipos = {}
        for coluna, tipo, in res.fetchall():
            table += "%s %s," % (coluna.strip(), tipo.strip())
            tipos[coluna.strip()] = tipo
        table = table[:-1]+");"
        return table, tipos


class migrador_oracle:
    def __init__(self, tabela):
        # dados da conexao de entrada
        self.user_origem = "PMJ"
        self.senha_origem = "PMJ"
        self.host_origem = "localhost"
        self.database_origem = "ORCL"
        self.schema_origem = "PMJ"

        # dados da conexao de saida
        self.user_destino = "postgres"
        self.senha_destino = "postgres"
        self.host_destino = "10.0.18.70"
        self.database_destino = "joanesia"
        self.porta_destino = 5433

        self.erros = []
        # variaveis de configuracao
        self.current = 0  # depreciada
        self.offset = 10000  # quantidade de inserts por ves

        # migra tabela especifica
        self.tabela = tabela
        if tabela != None:
            self.filtro = " and table_name = '%s' " % tabela
        else:
            self.filtro = ""

    def connect(self):
        """ CONECTA AOS BANCOS DE ENTRADA E SAIDA"""
        self.con_origem = cx_Oracle.connect(
            "%s/%s@%s/%s" % (self.user_origem, self.senha_origem, self.host_origem, self.database_origem))

        self.con_destino = psycopg2.connect(
            "user=%s password=%s host=%s port=%s dbname=%s" % (self.user_destino, self.senha_destino, self.host_destino, self.porta_destino, self.database_destino))

        self.cur_origem = self.con_origem.cursor()
        self.cur_destino = self.con_destino.cursor()

        self.con_destino.autocommit = True

    def migrar(self):
        """  COPIA A(S) TABELA(S) PARA O BANCO DE SAIDA """
        start = time.time()
        if not os.path.exists('output'):
            os.makedirs('output')
        # seleciona as tabelas
        res = self.cur_origem.execute(
            "SELECT table_name FROM dba_tables WHERE owner = '%s' %s order by table_name" % (self.schema_origem, self.filtro))

        # para cada tabela
        for row, in res.fetchall():
            row = row.strip()
            # conta os registros
            countsql = self.cur_origem.execute(
                "select count(*) as total from %s " % row)
            count, = countsql.fetchall()[0]
            start_time = time.time()
            start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print "MIGRANDO: %s\n          NRO REGISTROS: %s registros\n          INICIO: %s" % (
                row, count, start_datetime)

            # gera o create table e trunca a tabela ( se ja existir )
            create, tipos = self.ddl_table(row)
            self.cur_destino.execute(create)
            self.cur_destino.execute("TRUNCATE TABLE %s" % row)

            # gera as colunas
            cols = ""
            # tratamento exclusivo SONNER
            for id, [col, tipo] in tipos.iteritems():
                if col == "SENHA" or (col == "DADOS" and row == "CADARQUIVODIGITAL"):
                    cols += "NULL AS %s," % col
                else:
                    cols += "%s," % col
            # padrao seria:
            # for id, [col, tipo] in tipos.iteritems():
            #    cols += "%s," % col
            # print "select %s from %s " % (cols[:-1], row)

            # busca os dados
            self.cur_origem.execute("select %s from %s " % (cols[:-1], row))

            # grava os dados no TXT
            with open("output/%s.txt" % row, "wb") as f:
                w = csv.writer(
                    f, delimiter='|', quotechar='"')
                try:

                    # execao SONNER (dados TEXT tipo cblob necessita de conversao anterior por conta de encoding)
                    if row == "CADPESSOACONTSOC":
                        for cada in self.cur_origem.fetchall():
                            cada = list(cada)
                            if cada[5] is not None:
                                cada[5] = cada[5].read().encode("latin-1")
                            w.writerow(cada)

                    else:
                        w.writerows(self.cur_origem.fetchall())
                except Exception as e:
                    self.erros.append(["%s" % row, e])
                    end_time = time.time()
                    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("\tFIM: %s\tTEMPO: %ss\tTABELA COM ERRO %s" %
                          (end_datetime, round(end_time-start_time, 0), e))

            # le o arquivo gravado e copia para o banco destino
            with open("output/%s.txt" % row, "r") as f:
                try:
                    self.cur_destino.copy_expert(
                        """COPY %s FROM STDIN WITH QUOTE '"' DELIMITER '|' NULL '' CSV """ % row, f)
                except Exception as e:
                    self.erros.append(["%s" % row, e])
                    end_time = time.time()
                    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("          FIM: %s\n          TEMPO: %ss\n          TABELA COM ERRO %s" %
                          (end_datetime, round(end_time-start_time, 0), e))
                else:
                    end_time = time.time()
                    end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("          FIM: %s\n          TEMPO: %ss\n          OK" %
                          (end_datetime, round(end_time-start_time, 0)))
        end = time.time()
        print("TEMPO GASTO: %s s" % (end-start))

    def printa_erros(self):
        """ retorna os erros encontrados durante a migracao """
        for cada in self.erros:
            print "%s\t%s\n--------------------------------------\n" % (
                cada[0], cada[1])

    def ddl_table(self, tabela):
        """ gera o ddl (create table e tipos das colunas)"""
        sql = """SELECT col.column_name AS coluna,
                    CASE
                    WHEN col.data_type LIKE 'NUMBER%%' THEN 'NUMERIC'
                    WHEN col.data_type LIKE 'NCHAR%%' THEN 'CHAR'
                    WHEN col.data_type LIKE 'VARCHAR2%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'NVARCHAR2%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'VARCHAR%%' THEN col.DATA_TYPE||'('||col.DATA_LENGTH||')'
                    WHEN col.data_type LIKE 'BLOB%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'CLOB%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'NCLOB%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'LONG%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'RAW%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'BFILE%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'LONG RAW%%' THEN 'TEXT'
                    WHEN col.data_type LIKE 'FLOAT%%' THEN 'NUMERIC'
                    WHEN col.data_type LIKE 'BINARY_FLOAT%%' THEN 'NUMERIC'
                    WHEN col.data_type LIKE 'BINARY_DOUBLE%%' THEN 'NUMERIC'
                    WHEN col.data_type LIKE 'TIMESTAMP%%' THEN 'TIMESTAMP'
                    WHEN col.data_type LIKE 'INTERVAL%%' THEN 'TEXT'
                    ELSE col.DATA_TYPE
                    END AS tipo,
                    col.column_id,
                    col.data_type
                FROM
                    all_tab_columns col
                WHERE
                    upper(table_name) = '%s'
                ORDER BY col.column_id""" % (tabela)
        res = self.cur_origem.execute(sql)
        table = "CREATE TABLE IF NOT EXISTS %s (" % tabela
        tipos = {}
        for coluna, tipo, id, data_type, in res.fetchall():
            # EXECOES (palavras reservadas no postgres)
            if coluna.strip() == "NATURAL":
                col = "NATURALDE"
            elif coluna.strip() == "SIMILAR":
                col = "SIMILARR"
            else:
                col = coluna
            table += "%s %s," % (col.strip(), tipo.strip())
            tipos[id] = [coluna.strip(), data_type]
        table = table[:-1]+");"
        return table, tipos


class migrador:
    def __init__(self, tipo, tabela):
        self.tipo = "oracle"
        self.tabela = tabela
        if not tipo is None:
            self.tipo = tipo

    def start(self):
        if self.tipo == "oracle":
            return migrador_oracle(self.tabela)
        elif self.tipo == "firebird":
            return migrador_firebird(self.tabela)


if __name__ == "__main__":
    # caso passa argumento tabela faz somente a tabela
    try:
        tabela = sys.argv[1]
    except:
        tabela = None

    # colunas separadas por virgula ex: python fb2pg.py TABELA COLUNAA,COLUNAB,COLUNAC
    try:
        coluna = sys.argv[2]
        colunas = coluna.split(",")
    except:
        coluna = None

    """ SELECIONAR A ORIGEM A SER MIGRADA """
    # instancia a classe migradora
    # migrador = migrador("oracle", tabela)  # oracle , firebird
    migrador = migrador("firebird", tabela)  # oracle , firebird

    migrar = migrador.start()

    """ CONFIGURAR BANCO DE DADOS DE DESTINO """
    # dados da conexao de saida (destino)
    migrar.user_destino = "postgres"
    migrar.senha_destino = "postgres"
    migrar.host_destino = "10.0.18.70"
    migrar.database_destino = "jaguaracu_memory"
    migrar.porta_destino = 5433

    """ COFIGURAR BANCO DE DADOS DE SAIDA """
    # dados da conexao de entrada (origem)

    # EXEMPLO ORACLE
    # migrar.user_origem = "PMJ"
    # migrar.senha_origem = "PMJ"
    # migrar.host_origem = "localhost"
    # migrar.database_origem = "ORCL"
    # migrar.schema_origem = "PMJ"

    # EXEMPLO FIREBIRD
    migrar.host_origem = 'localhost'
    migrar.database_origem = 'C:/Users/dabes/Downloads/dbjaguaracu.fdb'
    migrar.user_origem = 'SYSDBA'
    migrar.senha_origem = 'masterkey'

    """ INICIA AS CONEXOES E MIGRA AO FINAL RETORNA OS ERROS """
    # conecta aos bancos origem e destino
    migrar.connect()
    # inicia a migracao
    migrar.migrar()
    # imprime os erros encontrados durante a migracao
    migrar.printa_erros()
