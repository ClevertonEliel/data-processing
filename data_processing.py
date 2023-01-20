import pandas as pd
import re
from pycpfcnpj import cpfcnpj as pj

def processing(input_path, output_path_estab, output_path_cnae):
    df_estab = pd.read_csv(input_path)

    #deletado colunas apos analise, verificado que existem dados com mais de 10% ausentes
    df_estab = df_estab.drop(['cod_situacao_especial', 'data_situacao_especial', 'fax', 'dddfax', 'telefone2', 'ddd2', 'telefone1', 'ddd1', 'end_complemento', 'end_cod_pais', 'end_nome_cidade_no_exterior', 'nome_fantasia'], axis=1)

    # adicionando 0 a esquerda para validacao de cnpj
    df_estab['cnpj_ordem'] = df_estab['cnpj_ordem'].apply(lambda x: str(x).zfill(4))
    df_estab.insert(0, 'cnpj','')
    df_estab['cnpj'] = df_estab[['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']].astype(str).apply(''.join, axis=1)

    #exluindo cnpjs com formato incorreto e filtrando apenas os validados
    df_estab = df_estab[df_estab['cnpj'].map(len) == 14].reset_index(drop=True)

    df_estab = df_estab[df_estab['cnpj'].map(lambda x: pj.validate(x))]

    #separando os codigos cnae secundarios a cada pipe ou virgula sem um limite de quantidade retornando uma series lista de str
    df_estab['cod_cnae_fiscal_secundaria'] = df_estab['cod_cnae_fiscal_secundaria'].str.split('[|,]', expand=False)
   
    #transformando a lista de cnae para cada linha
    df_estab = df_estab.explode('cod_cnae_fiscal_secundaria').reset_index(drop=True)

    #ajuste email - caracteres minusculos e com underscore
    df_estab['email'] = df_estab['email'].map(lambda x: (str(x).lower().replace('-','_').replace("'", "") if pd.notnull(x) else x))

    #ajuste cep, removendo dois ultimos caracteres(.0), adicionado 0 a esquerda quando neceessario
    df_estab['end_cep'] = df_estab['end_cep'].apply(lambda x: str(x)[:-2] if pd.notnull(x) else x)
    df_estab['end_cep'] = df_estab['end_cep'].apply(lambda x: str(x).zfill(8) if pd.notnull(x) else x)

    #padronizando para maiusculo
    df_estab['end_uf'] = df_estab['end_uf'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)

    #criando tabela aux com cnpj basico e cnae secundario
    df_cnae_secondary = df_estab[['cnpj_basico', 'cod_cnae_fiscal_secundaria']]

    #deletando coluna cnae secundario da tabela estab
    df_estab = df_estab.drop('cod_cnae_fiscal_secundaria', axis=1)

    #excluindo linhas duplicadas
    df_estab = df_estab.drop_duplicates()

    #criando planilha csv establishment
    df_estab['cnpj_basico'] = df_estab['cnpj_basico'].astype('str')
    df_estab.to_csv(output_path_estab)

    # # alterando valores 'nulo' e 'em branco' para NaN
    df_cnae_secondary.loc[df_cnae_secondary.cod_cnae_fiscal_secundaria == 'nulo', 'cod_cnae_fiscal_secundaria'] = float("NaN")
    df_cnae_secondary.loc[df_cnae_secondary.cod_cnae_fiscal_secundaria == 'em branco', 'cod_cnae_fiscal_secundaria'] = float("NaN")

    #ler tabela com codigos cnae baixada da receita para validação
    cnae_reports = pd.read_excel('tabela-cnae.xlsx')

    #remover .0 dos cod cnae da tabela de codigos
    cnae_reports['CNAE'] = cnae_reports['CNAE'].apply(lambda x: str(x)[:-2] if pd.notnull(x) else x)
    #remover 0 a esquerda do dataframe dos codigos cnae secundarios
    df_cnae_secondary['cod_cnae_fiscal_secundaria'] = df_cnae_secondary['cod_cnae_fiscal_secundaria'].apply(lambda x: re.sub('^(0+)', '', str(x)) if pd.notnull(x) else x)

    #verificado que existiam codigos cnae com valores inexistentes, alterado para Not a Number
    df_cnae_secondary['cod_cnae_fiscal_secundaria'].loc[df_cnae_secondary['cod_cnae_fiscal_secundaria'] == '1111111'] = float("NaN")
    df_cnae_secondary['cod_cnae_fiscal_secundaria'].loc[df_cnae_secondary['cod_cnae_fiscal_secundaria'] == '999999'] = float("NaN")
    df_cnae_secondary['cod_cnae_fiscal_secundaria'].loc[df_cnae_secondary['cod_cnae_fiscal_secundaria'] == '888888'] = float("NaN")

    #validação de codigos cnae, count para verificar quantos estao invalidos 
    count = 0
    for idx, cnae in enumerate(df_cnae_secondary['cod_cnae_fiscal_secundaria']):
        if not str(cnae) in cnae_reports['CNAE'].values and pd.notnull(cnae):
            #cnae nao encontrado na tabela extraida do site, mas apos pesquisa, é um cnae valido
            if str(cnae) == '3317102': continue
            
            df_cnae_secondary['cod_cnae_fiscal_secundaria'][idx] = float("NaN")
            count += 1
    print(count)

    #criando planilha cnae secundario
    df_cnae_secondary['cnpj_basico'] = df_cnae_secondary['cnpj_basico'].astype('str')
    df_cnae_secondary.to_csv(output_path_cnae)

processing('estab-part-00.csv', 'establishment.csv', 'cnae_secondary.csv')