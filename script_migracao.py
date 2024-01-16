import cx_Oracle
import os
import uuid

def conectarOracle(schema, password, host, serviceName):
    url = schema + '/' + password + '@' + host + '/' + serviceName
    connection = cx_Oracle.connect(url)
    print('Conectado ao Oracle! ' + connection.version)
    return connection

def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)

def criarDiretorioArquivo(path, ano, mes, dia, data):
    caminho = path + ano + '/' + mes + '/' + dia + '/'
    arquivo = caminho + str(uuid.uuid1()) + '.pdf'
    if not os.path.exists(caminho):
        os.makedirs(caminho)
    if not os.path.exists(arquivo):
        write_file(data, arquivo)
        return arquivo

def main():

    host = input('Digite o host do database: ')
    serviceName = input('Digite o service name: ')
    schema = input('Digite o schema: ')
    password = input('Digite a senha: ')
    path = input('Digite o path: ')

    try:

        print('++++++++++++++++++++++++++++++++++++++++++++++')
        con = conectarOracle(schema, password, host, serviceName)
        cur = con.cursor()
        sql = 'SELECT cod_arquivo, dthinclusao, arquivo FROM tb_arquivo'
        cur.execute(sql)

	#contador para saber a quantidade de arquivos gravados no sistema de arquivos
        cont = 0
        for s_cod, s_dthinclusao, s_arquivo in cur:
            s_data = s_dthinclusao.strftime('%Y/%m/%d')

            file = criarDiretorioArquivo(path, s_data[:4], s_data[5:7], s_data[8:10], s_arquivo.read())
            print('++++++++++++++++++++++++++++++++++++++++++++++') 
            print(file)

            cur = con.cursor()
            sqlUpdate = 'UPDATE tb_arquivo SET ds_path_arquivo = :1, ds_arquivo = :2 WHERE cod_arquivo = :3'
            cur.execute(sqlUpdate, (file[:-41], file[-40:], s_cod))

	    cont+=1
            print('Arquivos migrados: ',cont)

	con.commit()
        cur.close()

	print('----------------------------------------------')
        print('Total de arquivos gravados no sistema de arquivos: ',cont)

    except cx_Oracle.DatabaseError as e:
        print('Erro: ', e)

if __name__ == "__main__":
    main()
