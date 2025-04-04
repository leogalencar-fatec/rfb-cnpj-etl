TABLE_FIELDS = {
    "empresa": {
        "cnpj_basico": "str",
        "razao_social": "str",
        "cod_natureza_juridica": "str",
        "cod_qualificacao_do_responsavel": "str",
        "capital_social": "float",
        "cod_porte": "str",
        "ente_federativo_responsavel": "str",
    },
    "estabelecimento": {
        "cnpj_basico": "str",
        "cnpj_ordem": "str",
        "cnpj_dv": "str",
        "cod_id_matriz_filial": "str",
        "nome_fantasia": "str",
        "cod_situacao_cadastral": "str",
        "data_situacao_cadastral": "date",
        "cod_motivo_situacao_cadastral": "str",
        "nome_cidade_exterior": "str",
        "cod_pais": "str",
        "data_inicio_atividade": "date",
        "cod_cnae_fiscal": "str",
        "cod_cnae_fiscal_secundario": "str",
        "tipo_logradouro": "str",
        "logradouro": "str",
        "numero": "str",
        "complemento": "str",
        "bairro": "str",
        "cep": "str",
        "uf": "str",
        "cod_municipio": "str",
        "ddd_1": "str",
        "telefone_1": "str",
        "ddd_2": "str",
        "telefone_2": "str",
        "ddd_fax": "str",
        "telefone_fax": "str",
        "correio_eletronico": "str",
        "situacao_especial": "str",
        "data_situacao_especial": "date",
    },
    "simples": {
        "cnpj_basico": "str",
        "opcao_pelo_simples": "str",
        "data_opcao_pelo_simples": "date",
        "data_exclusao_pelo_simples": "date",
        "opcao_pelo_mei": "str",
        "data_opcao_pelo_mei": "date",
        "data_exclusao_pelo_mei": "date",
    },
    "socio": {
        "cnpj_basico": "str",
        "cod_id_socio": "str",
        "razao_social": "str",
        "cnpj_cpf_socio": "str",
        "cod_qualificacao_socio": "str",
        "data_entrada_sociedade": "date",
        "cod_pais_socio_estrangeiro": "str",
        "numero_cpf_representante_legal": "str",
        "nome_representante_legal": "str",
        "cod_qualificacao_representante_legal": "str",
        "cod_faixa_etaria": "str",
    },
    "pais": {"codigo": "str", "descricao": "str"},
    "municipio": {"codigo": "str", "descricao": "str"},
    "qualificacao_socio": {"codigo": "str", "descricao": "str"},
    "natureza_juridica": {"codigo": "str", "descricao": "str"},
    "cnae": {"codigo": "str", "descricao": "str"},
    "motivo": {"codigo": "str", "descricao": "str"},
}