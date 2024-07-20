import psycopg2
from cyvcf2 import VCF

# Define the function to insert variants into PostgreSQL
def insert_variant_postgres(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    vcf_file = kwargs['vcf_file']
    vcf = VCF(vcf_file)

    try:
        for variant in vcf:
            chrom, pos, ref, alt = variant.CHROM, variant.POS, variant.REF, variant.ALT
            qual, filter, info = variant.QUAL, variant.FILTER, dict(variant.INFO)
            if variant.ID is None:
                variant.ID = f'{variant.CHROM}-{variant.POS}-{variant.REF}-{variant.ALT}'

            cur.execute("""
                INSERT INTO variant(id, name, chrom, pos, ref, alt, qual, filter)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (variant.ID, variant.ID, chrom, pos, ref, alt, qual, filter))
            conn.commit()

            for i, sample in enumerate(vcf.samples):
                cur.execute("""
                    INSERT INTO sample(id, name)
                    VALUES(%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (sample, sample))
                conn.commit()

                ad_values = ','.join(map(str, variant.format('AD')[i])).replace(',', ' ')
                af_values = ','.join(map(str, variant.format('AF')[i]))
                gt_values = variant.genotypes[i][0]

                cur.execute("""
                    INSERT INTO format(variant_id, sample_id, allelic_depth, allele_frequency, genotype)
                    VALUES(%s, %s, %s, %s, %s)
                    ON CONFLICT (variant_id, sample_id) DO NOTHING
                """, (variant.ID, sample, ad_values, af_values, gt_values))
                conn.commit()
    except Exception as e:
        print(f"Error inserting variants: {e}")
    finally:
        cur.close()
        conn.close()
