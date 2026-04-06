-- TPCH10：PostgreSQL 导入脚本（由 psql -f 执行，使用客户端 \copy）
-- 数据路径：/data5/dzh/project/pgvector/data/tpch10/

-- 以下为历史 MySQL / OceanBase 语法备份，勿在 PG 中执行
-- LOAD DATA INFILE / PARTITION BY RANGE COLUMNS / DELIMITER 等见 Git 历史

BEGIN;

CREATE TABLE region  ( R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,
                       R_NAME       CHAR(25) NOT NULL,
                       R_COMMENT    VARCHAR(152));

\copy region FROM '/data/dzh/pgvector/data/tpch10/region.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

BEGIN;

CREATE TABLE nation  ( N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY,
                       N_NAME       CHAR(25) NOT NULL,
                       N_REGIONKEY  INTEGER NOT NULL,
                       N_COMMENT    VARCHAR(152));

\copy nation FROM '/data/dzh/pgvector/data/tpch10/nation.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

BEGIN;

CREATE TABLE supplier ( S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,
                        S_NAME        CHAR(25) NOT NULL,
                        S_ADDRESS     VARCHAR(40) NOT NULL,
                        S_NATIONKEY   INTEGER NOT NULL,
                        S_PHONE       CHAR(15) NOT NULL,
                        S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                        S_COMMENT     VARCHAR(101) NOT NULL);

\copy supplier FROM '/data/dzh/pgvector/data/tpch10/supplier.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

BEGIN;

CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY,
                        C_NAME        VARCHAR(25) NOT NULL,
                        C_ADDRESS     VARCHAR(40) NOT NULL,
                        C_NATIONKEY   INTEGER NOT NULL,
                        C_PHONE       CHAR(15) NOT NULL,
                        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                        C_MKTSEGMENT  CHAR(10) NOT NULL,
                        C_COMMENT     VARCHAR(117) NOT NULL);

\copy customer FROM '/data/dzh/pgvector/data/tpch10/customer.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

BEGIN;

CREATE TABLE orders (
    O_ORDERKEY      INTEGER NOT NULL PRIMARY KEY,
    O_CUSTKEY       INTEGER NOT NULL,
    O_ORDERSTATUS   CHAR(1) NOT NULL,
    O_TOTALPRICE    DECIMAL(15,2) NOT NULL,
    O_ORDERDATE     DATE NOT NULL,
    O_ORDERPRIORITY CHAR(15) NOT NULL,
    O_CLERK         CHAR(15) NOT NULL,
    O_SHIPPRIORITY  INTEGER NOT NULL,
    O_COMMENT       VARCHAR(79) NOT NULL
);

\copy orders FROM '/data/dzh/pgvector/data/tpch10/orders.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

BEGIN;

CREATE TABLE lineitem (
    L_ORDERKEY      INTEGER NOT NULL,
    L_PARTKEY       INTEGER NOT NULL,
    L_SUPPKEY       INTEGER NOT NULL,
    L_LINENUMBER    INTEGER NOT NULL,
    L_QUANTITY      DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL,
    L_DISCOUNT      DECIMAL(15,2) NOT NULL,
    L_TAX           DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG    CHAR(1) NOT NULL,
    L_LINESTATUS    CHAR(1) NOT NULL,
    L_SHIPDATE      DATE NOT NULL,
    L_COMMITDATE    DATE NOT NULL,
    L_RECEIPTDATE   DATE NOT NULL,
    L_SHIPINSTRUCT  CHAR(25) NOT NULL,
    L_SHIPMODE      CHAR(10) NOT NULL,
    L_COMMENT       VARCHAR(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);

\copy lineitem FROM '/data/dzh/pgvector/data/tpch10/lineitem.csv' WITH (FORMAT text, DELIMITER '|', NULL '')

COMMIT;

CREATE EXTENSION IF NOT EXISTS vector;

BEGIN;

-- 1. 创建 PART 表
-- 注意：DECIMAL 在 PG 中建议指定精度，或者直接用 NUMERIC
CREATE TABLE PART(
    p_partkey      INTEGER NOT NULL,
    p_name         VARCHAR(55),
    p_mfgr         VARCHAR(25),
    p_brand        VARCHAR(10),
    p_type         VARCHAR(25),
    p_size         INTEGER,
    p_container    VARCHAR(10),
    p_retailprice  DECIMAL(15,2),
    p_comment      VARCHAR(23),
    text_embedding vector(768)
);

-- 2. 使用匿名代码块执行循环加载
-- PG 不一定要创建存储过程再删除，可以直接用 DO 块
DO $$
DECLARE
    i INT := 0;
    file_path TEXT;
BEGIN
    FOR i IN 0..99 LOOP
        file_path := '/data/dzh/pgvector/data/vector_load_tpch10/part_with_vec_' || i || '.csv';
        
        -- 核心改动：在列名列表最后增加一个 dummy 变量名（比如叫 trailing_null）
        -- 这要求临时定义这个列，或者使用下面的方式
        
        EXECUTE format('
            CREATE TEMP TABLE temp_part_load (
                c1 INTEGER, c2 VARCHAR, c3 VARCHAR, c4 VARCHAR, c5 VARCHAR, 
                c6 INTEGER, c7 VARCHAR, c8 DECIMAL, c9 VARCHAR, c10 vector(768),
                trailing_null TEXT -- 这个列用来吸收行尾那个多余的 | 分隔符产生的空数据
            ) ON COMMIT DROP;

            COPY temp_part_load FROM %L WITH (FORMAT csv, DELIMITER ''|'', ENCODING ''UTF8'');

            INSERT INTO PART (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, text_embedding)
            SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 FROM temp_part_load;

            TRUNCATE temp_part_load;
            DROP TABLE temp_part_load;
        ', file_path);

        IF i % 10 = 0 THEN
            RAISE NOTICE '已加载 Part 文件: %', i;
        END IF;
    END LOOP;
END $$;

COMMIT;


BEGIN;

-- 1. 创建表（确保已执行 CREATE EXTENSION IF NOT EXISTS vector;）
CREATE TABLE PARTSUPP (
    ps_partkey          INTEGER NOT NULL,
    ps_suppkey          INTEGER NOT NULL,
    ps_availqty         INTEGER,
    ps_supplycost       DECIMAL(15,2),
    ps_comment          VARCHAR(199),
    ps_image_embedding  vector(96),
    ps_text_embedding   vector(768)
);

-- 2. 执行批量导入
DO $$
DECLARE
    i INT;
    file_path TEXT;
BEGIN
    FOR i IN 0..399 LOOP
        file_path := '/data/dzh/pgvector/data/vector_load_tpch10/partsupp_with_vec_' || i || '.csv';
        
        -- 1. 创建结构与 CSV 完全对应的临时表
        -- c6 对应 CSV 中的 image 向量位置，c7 对应 text 向量位置
        -- c8 用来吸收行尾多余的 '|'
        CREATE TEMP TABLE temp_ps_load (
            c1 INT, 
            c2 INT, 
            c3 INT, 
            c4 DECIMAL, 
            c5 TEXT, 
            c6 TEXT,   -- 对应 CSV 里的 image 向量数据（即便我们要丢弃它）
            c7 vector(768), 
            c8 TEXT    -- 吸收尾部空列
        ) ON COMMIT DROP;

        -- 2. 将 CSV 数据读入临时表
        EXECUTE format('COPY temp_ps_load FROM %L WITH (FORMAT csv, DELIMITER ''|'', ENCODING ''UTF8'')', file_path);
        
        -- 3. 按照业务逻辑插入正式表
        -- 这里显式将 ps_image_embedding 设为 NULL，并取 c7 作为 text_embedding
        INSERT INTO PARTSUPP (
            ps_partkey, 
            ps_suppkey, 
            ps_availqty, 
            ps_supplycost, 
            ps_comment, 
            ps_image_embedding, 
            ps_text_embedding
        )
        SELECT c1, c2, c3, c4, c5, NULL, c7 FROM temp_ps_load;
        
        -- 4. 清理临时表进入下一次循环
        DROP TABLE temp_ps_load;

        -- 每 50 个文件打印一次进度
        IF i % 50 = 0 THEN
            RAISE NOTICE 'PARTSUPP 加载进度: % / 400', i;
        END IF;
    END LOOP;
END $$;

COMMIT;