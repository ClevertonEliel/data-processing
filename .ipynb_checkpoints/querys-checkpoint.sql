-- SELECT * FROM empresas

-- CREATE VIEW VW_WITHOUT_CNAE_PR AS
-- SELECT COUNT(end_uf) FROM empresas
-- WHERE cod_cnae_fiscal_secundaria ISNULL and end_uf = 'PR'
-- SELECT * FROM vw_without_cnae_pr

-- CREATE VIEW VW_CNAE_VEHICLE_PARTS AS
-- SELECT COUNT(cod_cnae_fiscal_secundaria) FROM empresas
-- WHERE cod_cnae_fiscal_secundaria = '4530703'
-- SELECT * FROM vw_cnae_vehicle_parts

SELECT
	cod_cnae_fiscal_secundaria,
	COUNT(*) AS total_cnae 
FROM empresas 
WHERE (cod_cnae_fiscal_secundaria NOTNULL) and (end_uf = 'SC' or end_uf = 'SP')
GROUP BY cod_cnae_fiscal_secundaria order by 2 desc limit 10

SELECT cnpj, count(*) FROM empresas 
	GROUP BY cnpj
	HAVING COUNT(*) > 1 
	
SELECT
	cnpj,
	count(*) as cnpj
FROM empresas
WHERE end_uf = 'SC' or end_uf = 'SP'
GROUP BY cnpj order by 2 desc limit 10