-- Lookup Tables
CREATE TABLE pais (
    codigo VARCHAR(3) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE municipio (
    codigo VARCHAR(4) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE qualificacao_socio (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE natureza_juridica (
    codigo VARCHAR(4) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE cnae (
    codigo VARCHAR(7) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE motivo (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE id_matriz_filial (
    codigo VARCHAR(1) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE id_porte_empresa (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE id_situacao_cadastral (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE id_socio (
    codigo VARCHAR(1) PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE id_faixa_etaria (
    codigo VARCHAR(1) PRIMARY KEY,
    descricao TEXT
);

-- Main Tables
CREATE TABLE empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT,
    cod_natureza_juridica VARCHAR(4),
    cod_qualificacao_do_responsavel VARCHAR(2),
    capital_social DECIMAL(15,2),
    cod_porte VARCHAR(2),
    ente_federativo_responsavel TEXT,
    INDEX idx_empresa_natureza (cod_natureza_juridica),
    INDEX idx_empresa_qualificacao (cod_qualificacao_do_responsavel),
    INDEX idx_empresa_porte (cod_porte)
);

CREATE TABLE estabelecimento (
    cnpj_basico VARCHAR(8),
    cnpj_ordem VARCHAR(4),
    cnpj_dv VARCHAR(2),
    cod_id_matriz_filial CHAR(1),
    nome_fantasia TEXT,
    cod_situacao_cadastral VARCHAR(2),
    data_situacao_cadastral DATE,
    cod_motivo_situacao_cadastral VARCHAR(2),
    nome_cidade_exterior TEXT,
    cod_pais VARCHAR(3),
    data_inicio_atividade DATE,
    cod_cnae_fiscal VARCHAR(7),
    cod_cnae_fiscal_secundario TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep VARCHAR(8),
    uf VARCHAR(2),
    cod_municipio VARCHAR(7),
    ddd_1 VARCHAR(2),
    telefone_1 VARCHAR(10),
    ddd_2 VARCHAR(2),
    telefone_2 VARCHAR(10),
    ddd_fax VARCHAR(2),
    telefone_fax VARCHAR(10),
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv, uf),
    INDEX idx_estabelecimento_municipio (cod_municipio),
    INDEX idx_estabelecimento_cnae (cod_cnae_fiscal),
    INDEX idx_estabelecimento_pais (cod_pais),
    INDEX idx_estabelecimento_matriz (cod_id_matriz_filial),
    INDEX idx_estabelecimento_situacao (cod_situacao_cadastral),
    INDEX idx_estabelecimento_motivo (cod_motivo_situacao_cadastral)
) PARTITION BY LIST COLUMNS(uf) (
    PARTITION p_norte VALUES IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'),
    PARTITION p_nordeste VALUES IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'),
    PARTITION p_centro_oeste VALUES IN ('DF', 'GO', 'MT', 'MS'),
    PARTITION p_sudeste VALUES IN ('ES', 'MG', 'RJ', 'SP'),
    PARTITION p_sul VALUES IN ('PR', 'RS', 'SC')
);

CREATE TABLE simples (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    opcao_pelo_simples VARCHAR(1),
    data_opcao_pelo_simples DATE,
    data_exclusao_pelo_simples DATE,
    opcao_pelo_mei VARCHAR(1),
    data_opcao_pelo_mei DATE,
    data_exclusao_pelo_mei DATE,
    INDEX idx_simples_empresa (cnpj_basico)
);

CREATE TABLE socio (
    cnpj_basico VARCHAR(8),
    cod_id_socio VARCHAR(2),
    razao_social TEXT,
    cnpj_cpf_socio VARCHAR(14),
    cod_qualificacao_socio VARCHAR(2),
    data_entrada_sociedade DATE,
    cod_pais_socio_estrangeiro VARCHAR(3),
    numero_cpf_representante_legal VARCHAR(11),
    nome_representante_legal TEXT,
    cod_qualificacao_representante_legal VARCHAR(2),
    faixa_etaria VARCHAR(2),
    PRIMARY KEY (cnpj_basico, cod_id_socio, cnpj_cpf_socio),
    INDEX idx_socio_pais (cod_pais_socio_estrangeiro),
    INDEX idx_socio_qualificacao (cod_qualificacao_socio),
    INDEX idx_socio_id (cod_id_socio),
    INDEX idx_socio_faixa_etaria (faixa_etaria)
);
