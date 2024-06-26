import vcf

def read_vcf(vcf_file):
    vcf_reader = vcf.Reader(open(vcf_file), "r")

    for record in vcf_reader:
        print(f"Chromosome: {record.CHROM}")
        print(f"Position: {record.POS}")
        print(f"Reference: {record.REF}")
        print(f"Alternate: {record.ALT}")
        print(f"Quality: {record.QUAL}")
        print(f"Filter: {record.FILTER}")
        print(f"ID: {record.ID}")
        print(f"Number of Samples: {len(record.samples)}\n")
        for sample in record.samples:
            genotype = sample.gt_bases  # Genotype (e.g., 'A/G', 'T/T')
            genotype_alleles = sample.gt_alleles  # Allele indices (e.g., [0, 1])
            other_fields = sample.data  # Access other fields (AD, DP, etc.)

            # Process the extracted data
            print(f"Sample: {sample.sample}, Genotype: {genotype}")
            print(f"Genotype Alleles: {genotype_alleles}")
            print(f"Other Fields: {other_fields}")


if __name__ == '__main__':
    read_vcf("test.vep.vcf")