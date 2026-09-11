CREATE TABLE IF NOT EXISTS default.word_counts
(
    word String,
    count UInt64
)
ENGINE = MergeTree
ORDER BY word;
