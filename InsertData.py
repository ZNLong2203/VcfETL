from cyvcf2 import VCF
import psycopg2

def create_table(conn, cur):
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
    
    CREATE TABLE IF NOT EXISTS sample(
        id TEXT PRIMARY KEY,
        name VARCHAR(255) 
    );
    
    CREATE TABLE IF NOT EXISTS format(
        id serial PRIMARY KEY,
        variant_id TEXT,
        sample_id TEXT,
        allelic_depth TEXT,
        allele_frequency FLOAT,
        genotype VARCHAR(255),
        FOREIGN KEY(variant_id) REFERENCES variant(id),
        FOREIGN KEY(sample_id) REFERENCES sample(id)
    );
    """)
    conn.commit()

def insert_variant(conn, cur, vcf):
    for variant in vcf:
        chrom, pos, ref, alt = variant.CHROM, variant.POS, variant.REF, variant.ALT
        qual, filter = variant.QUAL, variant.FILTER
        # info = dict(variant.INFO)
        if(variant.ID == None):
            cur.execute("""
                SELECT COUNT(*) FROM variant;
            """)
            variant.ID = str(cur.fetchone()[0] + 1)

        cur.execute("""
            INSERT INTO variant(id, name, chrom, pos, ref, alt, qual, filter)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """, (variant.ID, variant.ID, chrom, pos, ref, alt, qual, filter))
        conn.commit()

        for i, sample in enumerate(vcf.samples):
            cur.execute("""
                            INSERT INTO sample(id, name)
                            VALUES(%s, %s)
                        """, (sample, sample))
            conn.commit()

            ad_values = ','.join(map(str, variant.format('AD')[i])).replace(',', ' ')
            af_values = ','.join(map(str, variant.format('AF')[i]))
            gt_values = variant.genotypes[0][i]
            print(ad_values, af_values, gt_values)
            cur.execute("""
                INSERT INTO format(variant_id, sample_id, allelic_depth, allele_frequency, genotype)
                VALUES(%s, %s, %s, %s, %s)
                """, (variant.ID, sample, ad_values, af_values, gt_values))
            conn.commit()


if __name__ == '__main__':
    vcf_file = "test.vep.vcf"
    vcf = VCF(vcf_file)
    conn = psycopg2.connect(
        host="localhost",
        database="vcf",
        user="postgres",
        password="test"
    )

    cur = conn.cursor()
    create_table(conn, cur)
    insert_variant(conn, cur, vcf)
