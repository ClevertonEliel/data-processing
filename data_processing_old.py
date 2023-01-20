import pandas as pd 
from pycpfcnpj import cpfcnpj as pj

def processing(input_path, output_path):
    df_estab = pd.read_csv(input_path)
    #retirando colunas sem necessidade
    #nr de valores vazios, usado para verificar quais colunas excluir
    #print(estab_df.isnull().sum())
    #estab_df.dropna(thresh=len(estab_df)*0.9, axis=1)
    df_estab = df_estab.drop(['cod_situacao_especial', 'data_situacao_especial', 'fax', 'dddfax', 'telefone2', 'ddd2', 'telefone1', 'ddd1', 'end_complemento', 'end_cod_pais', 'end_nome_cidade_no_exterior', 'nome_fantasia'], axis=1)

    # adicionando 0 a esquerda para validacao de cnpj
    df_estab['cnpj_ordem'] = df_estab['cnpj_ordem'].apply(lambda x: str(x).zfill(4))
    df_estab.insert(0, 'cnpj','')
    df_estab['cnpj'] = df_estab[['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']].astype(str).apply(''.join, axis=1)

    #exluindo cnpjs com formato incorreto e filtrando apenas os validados
    df_estab = df_estab[df_estab['cnpj'].map(len) == 14].reset_index(drop=True)
    
    df_estab = df_estab[df_estab['cnpj'].map(lambda x: pj.validate(x))]

    #split nos codigos cnae
    df_estab['cod_cnae_fiscal_secundaria'] = df_estab['cod_cnae_fiscal_secundaria'].str.split('[|,]', expand=False)
    df_estab = df_estab.explode('cod_cnae_fiscal_secundaria').reset_index(drop=True)

    #ajuste email - caracteres minusculos e com underscore
    df_estab['email'] = df_estab['email'].map(lambda x: (str(x).lower().replace('-','_').replace("'", "") if pd.notnull(x) else x))

    #ajuste cep
    df_estab['end_cep'] = df_estab['end_cep'].apply(lambda x: str(x)[:-2] if pd.notnull(x) else x)
    df_estab['end_cep'] = df_estab['end_cep'].apply(lambda x: str(x).zfill(8) if pd.notnull(x) else x)
    df_estab['end_uf'] = df_estab['end_uf'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)

    # criando arquivo csv para insert postgres lendo csv
    df_estab.to_csv(output_path)

    return output_path

processing('estab-part-00.csv', 'df_extracted_final.csv')