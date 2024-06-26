import time
from os import path

from cyvcf2 import VCF

from es_interaction.big_snp_var_dba import big_snp_var_dba
from pattern.abstract_factory.es_init import EsInit


class vcf_snp_singlefile_parser(EsInit):

    def __init__(self, vcf_file, subject_index="", submiter_id=""):
        super().__init__()
        self.subject_index = subject_index
        self.submiter_id = submiter_id
        self.vcf_file_path = vcf_file


    def check_file(self):
        #file validation will be implemented here
        if path.exists(self.vcf_file_path):
            return True
        else:
            return False

    def get_minimal_representation(self,pos, ref, alt):
        """
        Get the minimal representation of a variant, based on the ref + alt alleles in a VCF
        This is used to make sure that multiallelic variants in different datasets,
        with different combinations of alternate alleles, can always be matched directly.
        Note that chromosome is ignored here - in xbrowse, we'll probably be dealing with 1D coordinates
        Args:
            pos (int): genomic position in a chromosome (1-based)
            ref (str): ref allele string
            alt (str): alt allele string
        Returns:
            tuple: (pos, ref, alt) of remapped coordinate
        """
        pos = int(pos)
        # If it's a simple SNV, don't remap anything
        if len(ref) == 1 and len(alt) == 1:
            return pos, ref, alt
        else:
            # strip off identical suffixes
            while (alt[-1] == ref[-1] and min(len(alt), len(ref)) > 1):
                alt = alt[:-1]
                ref = ref[:-1]
            # strip off identical prefixes and increment position
            while (alt[0] == ref[0] and min(len(alt), len(ref)) > 1):
                alt = alt[1:]
                ref = ref[1:]
                pos += 1
            return pos, ref, alt

    def file_show(self):
        if self.check_file()==False:
            print(" error with given file. Please check file.")
        else:
            vcf = VCF(self.vcf_file_path)
            for variant in vcf:
                print("Chromosome:", variant.CHROM)
                print("Position:", variant.POS)
                print("Reference allele:", variant.REF)
                print("Alternate allele(s):", variant.ALT)
                print("variant_id: ", variant.ID)
                print("Qual: ", variant.QUAL)
                print("Filter: ", variant.FILTER)
                print("Additional inf: ", variant.INFO)
                print()

    def file_processing(self, number_threads=1):

        if self.check_file()==False:
            print(" error with given file. Please check file.")
            self.parser_succeed=False
        else:

            vcf_obj = VCF(self.vcf_file_path)
            vcf_obj.set_threads(number_threads)

            # print("samples: ", vcf_obj.samples)
            num_samples = len(vcf_obj.samples)
            print("Number of samples: ", num_samples)
            samples = vcf_obj.samples
            #############################################
            print("Sample: ", samples[0])
            subject_inf = {
                "samplecode": samples[0],
                # cac thong tin mac dinh
                "subject_id": self.subject_index,
                "submitter_id": self.submiter_id
            }
            ################################################
            #### xử lý thông tin các biến (variants) và phân tách vào các index
            try:
                start_time = time.time()
                ###### đọc dữ liệu từ file vcf cho các biến :
                number_chromosome = 0
                curre_ch = ''
                subject_variant_list_buffer = []

                print(" variant list procesing")
                for variant in vcf_obj:
                    #### kiểm tra xem đã sang chromosome mới chưa và xử lý
                    if curre_ch != str(variant.CHROM):
                        if number_chromosome == 0:
                            ### xu ly chromosome dau tien
                            number_chromosome += 1
                            curre_ch = str(variant.CHROM)
                        else:
                            ### update du lieu của chromosome trước đó lên database
                            try:
                                ### insert to elasticsearch ##########
                                index_name_org = "vin_snp_id" + "_" + curre_ch
                                index_name = index_name_org.lower()
                                print("Index name: ", index_name)
                                if self.es.indices.exists(index=index_name) == False:
                                    ### tao index
                                    setting = {
                                        'settings': {
                                            'number_of_shards': 5,
                                            'number_of_replicas': 3
                                        },
                                        'mappings': {
                                            'properties': {
                                                'submitter_id': {'type': 'text'},
                                                'subject_id': {'type': 'text'},
                                                'samplecode': {'type': 'text'},
                                            }
                                        }
                                    }
                                    try:
                                        self.es.indices.create(index=index_name, body=setting)
                                        print("create index: ", index_name)
                                    except Exception as ela_err:
                                        print("create index error. {}".format(ela_err))
                                        print("create index error")
                                else:
                                    print("db exist.")
                                #################################
                                ### insert to db
                                dba_obj = big_snp_var_dba(index=index_name)

                                ### chen du lieu cua sample vao theo tung bien
                                print(" insert sample: ",  samples[0])
                                ###''' so thu tu trong danh sách samples cần trừ lui đi sta_sam_pos để lấy chỉ số mang'''###
                                subject_inf["variant_list"] = subject_variant_list_buffer
                                # print(subject_list[idx])
                                print(dba_obj.ins_snp(subject_inf))

                                ###clear variant from a chromosome after insert
                                subject_variant_list_buffer.clear()
                            except Exception as ela_err:
                                print(ela_err)
                            ### chuyen sang nhiem sac the moi
                            curre_ch = str(variant.CHROM)
                            number_chromosome += 1
                    ######################################
                    ### xử lý thong tin từng biến
                    allele_pair = [variant.REF, variant.ALT]
                    for i, a in enumerate(allele_pair):
                        allele_pair[i] = '' if not a else a
                        if isinstance(a, str):
                            allele_pair[i] = [a]
                    ref_val, alt_val = allele_pair
                    for rv in ref_val:
                        for i, va in enumerate(alt_val):
                            pos, ref, alt = self.get_minimal_representation(int(variant.POS), str(rv), str(va))
                            vid = '-'.join((str(variant.CHROM), str(pos), ref, alt))
                            # print(" genome id: ", vid)
                            # print(" is snp ", variant.is_snp)
                            ########################################
                            ### lay cho sample đầu tiên
                            # print("Chromosome:", variant.CHROM)
                            # print("variant_id: ", variant.ID)
                            # print("Position:", variant.POS)
                            # print("Reference allele:", variant.REF)
                            # print("Alternate allele(s):", variant.ALT)
                            # print("Qual: ", variant.QUAL)
                            # print("Filter: ", variant.FILTER)
                            # print("AF inf: ", variant.INFO.get('AF'))
                            # print("GT inf: ", variant.gt_types[idx])
                            sub_variant_doc = {}
                            sub_variant_doc["variant_id"] = vid
                            sub_variant_doc["chromosome"] = str(variant.CHROM)
                            sub_variant_doc["position"] = variant.POS
                            sub_variant_doc["id"] = str(variant.ID)
                            sub_variant_doc["AF"] = variant.INFO.get('AF')
                            sub_variant_doc["AN"] = variant.INFO.get('AN')
                            sub_variant_doc["GT"] = str(variant.genotypes[0])
                            # variant_doc["quality"] = variant.QUAL
                            # variant_doc["filter"] = variant.FILTER
                            ### them vao du lieu của chromosome tuong ung cho moi sample
                            subject_variant_list_buffer.append(sub_variant_doc)

                ##############
                end_time = time.time()
                print(" processing time ", end_time - start_time)

            except Exception as err:
                ### thay cho ghi log file
                print("loi xu ly file vcf.")
                print(err)





# if __name__ == "__main__":
#     vcf_path = "./data_demo/sample.vcf"
#     vcfparserObj = vcf_snp_singlefile_Parser(vcf_file=vcf_path,subject_index="0001")
#     # vcfparserObj.file_show()
#     subjectObj = vcfparserObj.file_processing(number_threads=2)
