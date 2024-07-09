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

    ac_values = variant.INFO.get('AC')
    an_values = variant.INFO.get('AN')
    end_values = variant.INFO.get('END')
    fs_values = variant.INFO.get('FS')
    lod_values = variant.INFO.get('LOD')
    mq_values = variant.INFO.get('MQ')
    qd_values = variant.INFO.get('QD')
    sor_values = variant.INFO.get('SOR')
    nml_values = variant.INFO.get('NML')
    fgt_values = variant.INFO.get('FGT')


    print(f"Allele Counts: {ac_values}")
    print(f"Allele Numbers: {an_values}")
    print(f"End: {end_values}")
    print(f"FS: {fs_values}")
    print(f"LOD: {lod_values}")
    print(f"MQ: {mq_values}")
    print(f"QD: {qd_values}")
    print(f"SOR: {sor_values}")
    print(f"NML: {nml_values}")
    print(f"FGT: {fgt_values}")

    print()

    # Custom Fields
    ad_values = variant.format('AD')
    af_values = variant.format('AF')
    gt_values = variant.genotypes[0]
    for i, sample in enumerate(vcf.samples):
        print(f"Sample {sample}")
        print(f"AD: {ad_values[i]}")
        print(f"AF: {af_values[i]}")
        print(f"GT: {gt_values[i]}")


