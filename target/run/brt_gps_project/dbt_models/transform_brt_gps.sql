
  create view "postgres"."public"."transform_brt_gps__dbt_tmp"
    
    
  as (
    SELECT
    (veiculo->>'codigo')::FLOAT AS id,
    (veiculo->>'latitude')::FLOAT AS latitude,
    (veiculo->>'longitude')::FLOAT AS longitude,
    (veiculo->>'velocidade')::FLOAT AS velocidade
FROM "postgres"."public"."brt_gps_data",
LATERAL jsonb_array_elements(
    replace(replace(replace(veiculos, '''', '"'), 'None', 'null'), 'D"Á', 'DA')::jsonb
) AS veiculo
  );