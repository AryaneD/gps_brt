SELECT
    (veiculo->>'codigo')::FLOAT AS id,
    (veiculo->>'latitude')::FLOAT AS latitude,
    (veiculo->>'longitude')::FLOAT AS longitude,
    (veiculo->>'velocidade')::FLOAT AS velocidade
FROM {{ source('public', 'brt_gps_data') }},
LATERAL jsonb_array_elements(
    replace(replace(replace(veiculos, '''', '"'), 'None', 'null'), 'D"Á', 'DA')::jsonb
) AS veiculo
