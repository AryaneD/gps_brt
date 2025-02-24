WITH veiculos_extracao AS (
    SELECT
        id,
        -- Desempacotando o JSON
        JSON_EXTRACT(veiculos, '$[0].latitude') AS latitude,
        JSON_EXTRACT(veiculos, '$[0].longitude') AS longitude,
        JSON_EXTRACT(veiculos, '$[0].velocidade') AS velocidade
    FROM {{ ref('brt_gps_data') }}
)
SELECT
    id,
    CAST(latitude AS FLOAT) AS latitude,
    CAST(longitude AS FLOAT) AS longitude,
    CAST(velocidade AS FLOAT) AS velocidade
FROM veiculos_extracao
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND velocidade IS NOT NULL;
