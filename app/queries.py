QUERY_FILMES = """
WITH filmes AS(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY nome_filme ORDER BY data DESC) AS ranking
FROM
[dbo].[Notas_Filmes]
WHERE
data >= DATEADD(DAY, -15, GETDATE())
)

SELECT
*,
ROW_NUMBER() OVER (PARTITION BY ranking ORDER BY nota_score DESC) AS posicao
FROM
filmes
WHERE
ranking = 1
ORDER BY nota_score DESC;
"""

QUERY_SERIES = """
WITH series AS(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY nome_serie ORDER BY data DESC) AS ranking
FROM
[dbo].[Notas_Series]
WHERE
data >= DATEADD(DAY, -15, GETDATE())
)

SELECT
*,
ROW_NUMBER() OVER (PARTITION BY ranking ORDER BY nota_score DESC) AS posicao
FROM
series
WHERE
ranking = 1
ORDER BY nota_score DESC;
"""


QUERY_DASH = """
WITH dados AS (
    SELECT
        COALESCE(s.streaming, f.streaming) AS streaming,
        COALESCE(s.data, f.data) AS data,
        CASE 
            WHEN s.nota_score IS NOT NULL AND f.nota_score IS NOT NULL THEN (s.nota_score + f.nota_score) / 2
            WHEN s.nota_score IS NOT NULL THEN s.nota_score
            WHEN f.nota_score IS NOT NULL THEN f.nota_score
            ELSE NULL
        END AS nota
    FROM
        [dbo].[Notas_Series] s
    FULL OUTER JOIN [dbo].[Notas_Filmes] f 
        ON s.streaming = f.streaming
        AND s.data = f.data
    WHERE
       s.data IS NOT NULL

)

SELECT
    streaming,
    data,
    ROUND(AVG(nota), 1) AS Media
FROM 
    dados
GROUP BY 
    data, streaming
ORDER BY 1,2;
"""