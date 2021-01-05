MIGRADOR GERAL PARA ORACLE E FIREBIRD PARA POSTGRESQL
PARA USAR BASTAR TER O BANCO DE ORIGEM RESTAURADO E SUAS CONFIGURACOES COLOCADAS CORRETAMENTE
                     O BANCO DE DESTINO CRIADO E SUAS CONFIGURACOES COLOCADAS CORRETAMENTE

CASO SEJA NECESSARIO MIGRAR APENAS UMA TABELA PODE-SE PASSAR COMO ARGUMENTO: python migrador.py TABELA

AO FINAL DO PROCESSO O MIGRADOR RETORNARÁ ERROS CASO OCORRAM POR TABELA

USA A ENGENHARIA DE COPY (GRAVA UM ARQUIVO), POIS SUA EFICIENCIA E MELHOR QUE O INSERT

NO FINAL DO SCRIPT TEM COMO SE CONFIGURA AS INFORMACOES DE BANCOS DE DADOS DE ORIGEM E DESTINO


    """ SELECIONAR A ORIGEM A SER MIGRADA """
    # instancia a classe migradora
    
    migrador = migrador("oracle", tabela)  # oracle 
    migrador = migrador("firebird", tabela)  # o firebird

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
    # migrar.user_origem = "USER"
    # migrar.senha_origem = "PASSWORD"
    # migrar.host_origem = "localhost"
    # migrar.database_origem = "DATABASE"
    # migrar.schema_origem = "SCHEMA"

    # EXEMPLO FIREBIRD
    migrar.host_origem = 'localhost'
    migrar.database_origem = 'database.fdb'
    migrar.user_origem = 'SYSDBA'
    migrar.senha_origem = 'PASSWORD'

    """ INICIA AS CONEXOES E MIGRA AO FINAL RETORNA OS ERROS """
    # conecta aos bancos origem e destino
    migrar.connect()
    # inicia a migracao
    migrar.migrar()
    # imprime os erros encontrados durante a migracao
    migrar.printa_erros()
