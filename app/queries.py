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