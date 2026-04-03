WITH deterministic AS (
    SELECT source_system_a, source_key_a, source_system_b, source_key_b,
           confidence, match_method
    FROM {{ ref('int_patient_match_deterministic') }}
),

probabilistic AS (
    SELECT source_system_a, source_key_a, source_system_b, source_key_b,
           confidence, match_method
    FROM {{ ref('int_patient_match_probabilistic') }}
),

all_matches AS (
    SELECT * FROM deterministic
    UNION ALL
    SELECT * FROM probabilistic
),

-- Deduplicate: if same pair appears in both sets, keep highest confidence
deduped_matches AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    LEAST(source_system_a || '::' || source_key_a, source_system_b || '::' || source_key_b),
                    GREATEST(source_system_a || '::' || source_key_a, source_system_b || '::' || source_key_b)
                ORDER BY confidence DESC
            ) AS rn
        FROM all_matches
    )
    WHERE rn = 1
),

final_matches AS (
    SELECT source_system_a, source_key_a, source_system_b, source_key_b,
           confidence, match_method
    FROM deduped_matches
),

-- Bidirectional adjacency list
adjacency AS (
    SELECT source_system_a || '::' || source_key_a AS node,
           source_system_b || '::' || source_key_b AS neighbor
    FROM final_matches
    UNION ALL
    SELECT source_system_b || '::' || source_key_b AS node,
           source_system_a || '::' || source_key_a AS neighbor
    FROM final_matches
),

-- All nodes from spine
all_nodes AS (
    SELECT source_system || '::' || source_patient_key AS node,
           source_system,
           source_patient_key
    FROM {{ ref('int_patient_spine') }}
),

-- Pass 0: each node starts with itself as cluster
pass_0 AS (
    SELECT node, node AS cluster_id FROM all_nodes
),

-- Pass 1: adopt minimum of own cluster and all neighbors' clusters
pass_1 AS (
    SELECT p.node,
           LEAST(p.cluster_id, COALESCE(MIN(n.cluster_id), p.cluster_id)) AS cluster_id
    FROM pass_0 p
    LEFT JOIN adjacency a ON p.node = a.node
    LEFT JOIN pass_0 n ON a.neighbor = n.node
    GROUP BY p.node, p.cluster_id
),

-- Pass 2
pass_2 AS (
    SELECT p.node,
           LEAST(p.cluster_id, COALESCE(MIN(n.cluster_id), p.cluster_id)) AS cluster_id
    FROM pass_1 p
    LEFT JOIN adjacency a ON p.node = a.node
    LEFT JOIN pass_1 n ON a.neighbor = n.node
    GROUP BY p.node, p.cluster_id
),

-- Pass 3
pass_3 AS (
    SELECT p.node,
           LEAST(p.cluster_id, COALESCE(MIN(n.cluster_id), p.cluster_id)) AS cluster_id
    FROM pass_2 p
    LEFT JOIN adjacency a ON p.node = a.node
    LEFT JOIN pass_2 n ON a.neighbor = n.node
    GROUP BY p.node, p.cluster_id
),

-- Pass 4
pass_4 AS (
    SELECT p.node,
           LEAST(p.cluster_id, COALESCE(MIN(n.cluster_id), p.cluster_id)) AS cluster_id
    FROM pass_3 p
    LEFT JOIN adjacency a ON p.node = a.node
    LEFT JOIN pass_3 n ON a.neighbor = n.node
    GROUP BY p.node, p.cluster_id
),

-- Assign canonical patient IDs
with_patient_id AS (
    SELECT
        p.node,
        p.cluster_id,
        'PAT-' || LPAD(CAST(DENSE_RANK() OVER (ORDER BY p.cluster_id) AS VARCHAR), 6, '0') AS patient_id,
        an.source_system,
        an.source_patient_key
    FROM pass_4 p
    JOIN all_nodes an ON p.node = an.node
),

-- Get best match info per node
edges_with_info AS (
    SELECT source_system_a || '::' || source_key_a AS node, confidence, match_method FROM final_matches
    UNION ALL
    SELECT source_system_b || '::' || source_key_b AS node, confidence, match_method FROM final_matches
),

node_best_match AS (
    SELECT node,
           MAX(confidence) AS best_confidence,
           ARG_MIN(match_method, -confidence) AS best_method
    FROM edges_with_info
    GROUP BY node
)

SELECT
    wp.source_system,
    wp.source_patient_key,
    wp.patient_id,
    wp.cluster_id,
    CAST(COALESCE(nbm.best_confidence, 1.000) AS DECIMAL(4,3)) AS match_confidence,
    COALESCE(nbm.best_method, 'unmatched_singleton') AS match_method
FROM with_patient_id wp
LEFT JOIN node_best_match nbm ON wp.node = nbm.node
