import psycopg2

def create_table(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS variant(
                id TEXT PRIMARY KEY,
                name VARCHAR(255),
                chrom VARCHAR(255),
                pos VARCHAR(255),
                ref VARCHAR(255),
                alt VARCHAR(255),
                qual FLOAT,
                filter VARCHAR(255),
                info JSONB
            );

            CREATE INDEX IF NOT EXISTS idx_variant_chrom ON variant(chrom);
            CREATE INDEX IF NOT EXISTS idx_variant_pos ON variant(pos);

            CREATE TABLE IF NOT EXISTS sample(
                id TEXT PRIMARY KEY,
                name VARCHAR(255)
            );

            CREATE INDEX IF NOT EXISTS idx_sample_name ON sample(name);

            CREATE TABLE IF NOT EXISTS format(
                id serial PRIMARY KEY,
                variant_id VARCHAR(255),
                sample_id VARCHAR(255),
                allelic_depth VARCHAR(255),
                allele_frequency FLOAT,
                genotype VARCHAR(255),
                FOREIGN KEY(variant_id) REFERENCES variant(id),
                FOREIGN KEY(sample_id) REFERENCES sample(id),
                UNIQUE(variant_id, sample_id)
            );
        """)
        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        cur.close()
        conn.close()
