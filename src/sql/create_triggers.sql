-- Replace invalid dates with NULL
CREATE TRIGGER before_update_default_null_estabelecimento
BEFORE INSERT ON estabelecimento
FOR EACH ROW
SET NEW.data_situacao_especial = IF(NEW.data_situacao_especial = '', NULL, NEW.data_situacao_especial),
    NEW.data_situacao_cadastral = IF(NEW.data_situacao_cadastral = '', NULL, NEW.data_situacao_cadastral),
    NEW.data_inicio_atividade = IF(NEW.data_inicio_atividade = '', NULL, NEW.data_inicio_atividade);


CREATE TRIGGER before_update_default_null_simples
BEFORE INSERT ON simples
FOR EACH ROW
SET NEW.data_opcao_pelo_simples = IF(NEW.data_opcao_pelo_simples = '', NULL, NEW.data_opcao_pelo_simples),
    NEW.data_exclusao_pelo_simples = IF(NEW.data_exclusao_pelo_simples = '', NULL, NEW.data_exclusao_pelo_simples),
    NEW.data_opcao_pelo_mei = IF(NEW.data_opcao_pelo_mei = '', NULL, NEW.data_opcao_pelo_mei),
    NEW.data_exclusao_pelo_mei = IF(NEW.data_exclusao_pelo_mei = '', NULL, NEW.data_exclusao_pelo_mei);


CREATE TRIGGER before_update_default_null_socio
BEFORE INSERT ON socio
FOR EACH ROW
SET NEW.data_entrada_sociedade = IF(NEW.data_entrada_sociedade = '', NULL, NEW.data_entrada_sociedade);
