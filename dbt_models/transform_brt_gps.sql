WITH veiculos_extracao AS (
    SELECT
        id,
        -- Desempacotando o JSON
        (veiculos->0->>'latitude')::FLOAT AS latitude,
        (veiculos->0->>'longitude')::FLOAT AS longitude,
        (veiculos->0->>'velocidade')::FLOAT AS velocidade
    FROM {{ source('public', 'brt_gps_data') }}
)
SELECT
    id,
    latitude,
    longitude,
    velocidade
FROM veiculos_extracao
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND velocidade IS NOT NULL;
