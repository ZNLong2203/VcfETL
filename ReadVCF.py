from cyvcf2 import VCF

vcf_file = "test.vep.vcf"

vcf = VCF(vcf_file)

# Header Information
print("Samples:", vcf.samples)

# Variant Iteration
for variant in vcf:
    # Basic Variant Info
    chrom, pos, ref, alt = variant.CHROM, variant.POS, variant.REF, variant.ALT
    print("Chrom:", chrom, "| Pos:", pos, "| Ref:", ref, "| Alt:", alt)
    print("ID:", variant.ID)
    print("QUALITY:", variant.QUAL)
    print("FILTER:", variant.FILTER)
    print(variant.FORMAT)

    # dp_values = variant.format('DP')  # Array of DP values for each sample
    # for i, sample in enumerate(vcf.samples):
    #     print(f"Sample {sample} - DP: {dp_values[i]}")

    print()

    # INFO Fields (Example: AC - Allele Count)
    ac_values = variant.INFO.get('AC')
    print(f"Allele Counts: {ac_values}")

    # Custom Fields
    ad_values = variant.format('AD')
    af_values = variant.format('AF')
    # an_values = variant.INFO.get('AN')
    gt_values = variant.genotypes[0]
    for i, sample in enumerate(vcf.samples):
        print(f"Sample {sample}")
        print(f"AD: {ad_values[i]}")
        print(f"AF: {af_values[i]}")
        # print(f"AN: {an_values[i]}")
        print(f"GT: {gt_values[i]}")


