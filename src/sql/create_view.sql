CREATE VIEW IF NOT EXISTS data_view AS
SELECT
    data_ingestao,
    JSONExtractString(dado_linha, 'name') AS name,
    JSONExtractInt(dado_linha, 'base_experience') AS base_experience,
    tag
FROM working_data;