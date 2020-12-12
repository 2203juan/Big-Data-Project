-- Se crea el grafo
LOAD CSV WITH HEADERS FROM 'http://localhost:11001/project-76d4894d-6d21-418f-953d-a0d3aa0b1d71/dataset_graph.csv' AS line FIELDTERMINATOR ';'
MERGE (a:Product {id:line.id1, category:toInteger(line.category1), colour:toInteger(line.colour1), photo:toInteger(line.photo1), price:toInteger(line.price1)})
MERGE (b:Product {id:line.id2, category:toInteger(line.category2), colour:toInteger(line.colour2), photo:toInteger(line.photo2), price:toInteger(line.price2)})
MERGE (a)-[r:COMPETES {w: toInteger(line.w)}]-(b)

-- Eliminamos las relaciones menores a 150 (nos quedamos con todas las relaciones >= 150)
MATCH (n)-[r:COMPETES]-()
WHERE r.w < 150
DELETE r

-- Eliminamos los nodos que quedan sin ningún tipo de conexión
MATCH (a) WHERE NOT (a)-[:COMPETES]-()
DELETE a

-- Ahora tenemos 167 nodos en lugar de 217 (Se borraron 50 nodos)

-- Generamos el degree para cada nodo
MATCH (a:Product)
SET a.degree = size((a)-[:COMPETES]-())

-- Esto solo retorna los degrees pero no los escribe
MATCH (a:Product)
RETURN a.id as id,
size((a)-[:COMPETES]-()) AS degree
ORDER BY degree DESC

-- Ver los degrees de cada nodo
MATCH (a:Product)
RETURN a.id, a.degree as deg
ORDER BY deg DESC

-- Se crea el schema de grafo con nombre para aplicar los algoritmos
CALL gds.graph.create(
    'myGraph',
    'Product',
    {
        COMPETES: {
            orientation: 'UNDIRECTED'
        }
    },
    {
        relationshipProperties: 'w'
    }
)

-- generamos el node2vec de tres dimensiones para cada nodo
CALL gds.alpha.node2vec.stream('myGraph', {embeddingDimension: 3})
YIELD nodeId, embedding
SET gds.util.asNode(nodeId).x = embedding[0], gds.util.asNode(nodeId).y = embedding[1], gds.util.asNode(nodeId).z = embedding[2]

-- Generamos el louvain_community para cada nodo segun Louvain (aquí tiene en cuenta el peso para hacer las comunidades. Sin tener en cuenta el peso
-- también funciona bien, pero se crea una comunidad extra con tan solo 4 nodos entonces mejor incluir el peso).
CALL gds.louvain.stream('myGraph', { relationshipWeightProperty: 'w' })
YIELD nodeId, communityId
SET gds.util.asNode(nodeId).louvain_comm = communityId

-- generamos el local clustering coefficient
CALL gds.localClusteringCoefficient.stream('myGraph')
YIELD nodeId, localClusteringCoefficient
SET gds.util.asNode(nodeId).clustering_coeff = localClusteringCoefficient

-- generamos betweennes score
CALL gds.betweenness.stream('myGraph')
YIELD nodeId, score
SET gds.util.asNode(nodeId).betweenness = score

-- generamos closeness score
CALL gds.alpha.closeness.stream({
  nodeProjection: 'Product',
  relationshipProjection: 'COMPETES'
})
YIELD nodeId, centrality
SET gds.util.asNode(nodeId).closeness = centrality





-- Me dice la cantidad de nodos en cáda comunidad de louvain
MATCH (a:Product)
RETURN a.community_id, Count(a.id)

-- eliminar un atributo de todos los nodos (por si hay que separar las metricas topológicas en dos bases de datos diferentes)
MATCH (a:Product)
REMOVE a.triangle_count

-- query para generar el csv
MATCH (a:Product)
RETURN a.id as id, a.louvain_community as louvain_comm, a.clustering_coeff as clustering_coeff, a.betweenness as betweenness, a.closeness as closeness, a.x as x, a.y as y, a.z as z
ORDER BY a.id ASC





-- Borrar el grafo nombrado
CALL gds.graph.drop('myGraph')

-- Borrar todos los nodos y relaciones
MATCH (n)
DETACH DELETE n

-- Este es un bug de la base de datos. El producto 'A18' no debería tener dos categorías diferentes.
-- Este lo borramos a mano
MATCH (a:Product), (b:Product)
WHERE a <> b and a.id = b.id
RETURN a, b

-- Con esto se mira la cantidad de nodos retornados por el filtro de pesos
MATCH (a)-[r:COMPETES]-(b)
WHERE r.w >= 200
RETURN Count(DISTINCT a)

-- Cantidad de relaciones
MATCH (a)-[r:COMPETES]-(b)
WHERE r.w >= 200
RETURN Count(r)
