// Esquema Neo4j del proyecto (idempotente: IF NOT EXISTS / MERGE).
// Modelo:
//   (:Usuario)-[:PUBLICA]->(:Post)
//   (:Usuario)-[:COMENTA]->(:Comentario)
//   (:Comentario)-[:PERTENECE_A]->(:Post)
//   (:Comentario)-[:RESPONDE_A]->(:Comentario)
//   (:Post)-[:EN]->(:Subreddit)
// Las restricciones de unicidad permiten que el ETL use MERGE sin duplicar nodos.

CREATE CONSTRAINT usuario_nombre IF NOT EXISTS FOR (u:Usuario) REQUIRE u.nombre IS UNIQUE;
CREATE CONSTRAINT post_id IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT comentario_id IF NOT EXISTS FOR (c:Comentario) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT subreddit_nombre IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.nombre IS UNIQUE;

CREATE INDEX post_fecha IF NOT EXISTS FOR (p:Post) ON (p.fecha);
CREATE INDEX comentario_fecha IF NOT EXISTS FOR (c:Comentario) ON (c.fecha);
CREATE INDEX post_score IF NOT EXISTS FOR (p:Post) ON (p.score);
// post_id / parent_id permiten enlazar comentarios ya cargados cuando su post o padre llega después.
CREATE INDEX comentario_post_id IF NOT EXISTS FOR (c:Comentario) ON (c.post_id);
CREATE INDEX comentario_parent_id IF NOT EXISTS FOR (c:Comentario) ON (c.parent_id);

// Nodos fijos: los 3 subreddits del dataset.
UNWIND ['LocalLLaMA', 'wallstreetbets', 'SkincareAddiction'] AS nombre
MERGE (:Subreddit {nombre: nombre});
