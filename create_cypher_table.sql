-- 创建cypher表，支持自增ID
CREATE TABLE IF NOT EXISTS cypher (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cypher TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 如果需要重置自增ID（谨慎使用）
-- ALTER TABLE cypher AUTO_INCREMENT = 1;

-- 查看表结构
DESCRIBE cypher; 