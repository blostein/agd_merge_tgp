version 1.0

# IMPORTS 

# WORKFLOW

workflow agd_combine_TGP {
    input { 
        File source_bed_file 
        File source_bim_file
        File source_fam_file 

        String source_chromosome 

        File thousand_genomes_psam_file
        File thousand_genomes_pvar_file
        File thousand_genomes_pgen_file

        File relatives_exclude
    }

    call SubsetChromosomeTGP {
        input: 
            pgen_file = thousand_genomes_pgen_file,
            pvar_file = thousand_genomes_pvar_file,
            psam_file = thousand_genomes_psam_file,
            chromosome = source_chromosome,
            relatives_exclude = relatives_exclude,
    }

    call Merge1000genomesAGD {
        input:
            agd_bed_file = source_bed_file,
            agd_bim_file = source_bim_file,
            agd_fam_file = source_fam_file,
            TGP_bed_file = SubsetChromosomeTGP.out_bed_file,
            TGP_bim_file = SubsetChromosomeTGP.out_bim_file,
            TGP_fam_file = SubsetChromosomeTGP.out_fam_file
    }

    output {
        File AGD_TGP_pgen_file = Merge1000genomesAGD.out_pgen_file
        File AGD_TGP_pvar_file = Merge1000genomesAGD.out_pvar_file
        File AGD_TGP_psak_file = Merge1000genomesAGD.out_psam_file
    }
}

# TASKS



task SubsetChromosomeTGP {
    input { 
        File pgen_file
        File pvar_file
        File psam_file 

        String chromosome

        File relatives_exclude

        Int? memory_gb = 20

        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    String out_string = "TGP_" + chromosome
    String in_chromosome = sub(chromosome, "chr", "")
    Int disk_size = ceil(size([pgen_file, pvar_file, psam_file], "GB")  * 2)*2 + 20

    
    runtime {
        docker: docker
        preemptible: 1
        disks: "local-disk " + disk_size + " HDD"
        memory: memory_gb + " GiB"
    }

    command{ 
         plink2 \
            --pgen ~{pgen_file} --pvar ~{pvar_file} --psam ~{psam_file} \
            --allow-extra-chr \
            --chr ~{in_chromosome} \
            --remove ~{relatives_exclude} \
            --make-bed \
            --out ~{out_string}
    }

    output{

        File out_bed_file = out_string + ".bed"
        File out_bim_file = out_string + ".bim"
        File out_fam_file = out_string + ".fam"

    }
}

task Merge1000genomesAGD{
    input{
        File agd_bed_file
        File agd_bim_file
        File agd_fam_file

        File TGP_bed_file
        File TGP_bim_file
        File TGP_fam_file

        String chromosome

        Int? memory_gb = 20


        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    Int disk_size = ceil(size([agd_bed_file, TGP_bed_file], "GB")  * 2)*2 + 20


    String out_string = "AGD_TGP_" + chromosome
    String agd_prefix = basename(agd_bed_file, ".bed")
    String TGP_prefix = basename(TGP_bed_file, ".bed")
    String agd_prefix_rename= agd_prefix + "_renamed"
    String agd_prefix_2 = agd_prefix + "_2"
    String TGP_prefix_2 = TGP_prefix + "_2"

    String relocated_bed = agd_prefix + ".bed"
    String relocated_bim = agd_prefix + ".bim"
    String relocated_fam = agd_prefix + ".fam"

    String relocated_tgp_bed = TGP_prefix + ".bed"
    String relocated_tgp_bim = TGP_prefix + ".bim"
    String relocated_tgp_fam = TGP_prefix + ".fam"


    runtime {
        docker: docker
        preemptible: 1
        disks: "local-disk " + disk_size + " HDD"
        memory: memory_gb + " GiB"
    }

    command{

        ln ~{agd_bed_file} ./~{relocated_bed}
        ln ~{agd_bim_file} ./~{relocated_bim}
        ln ~{agd_fam_file} ./~{relocated_fam}

        ln ~{TGP_bed_file} ./~{relocated_tgp_bed}
        ln ~{TGP_bim_file} ./~{relocated_tgp_bim}
        ln ~{TGP_fam_file} ./~{relocated_tgp_fam}

        plink2 \
            --bfile ~{agd_prefix} \
            --set-all-var-ids @:#:\$r:\$a \
            --new-id-max-allele-len 1000 \
            --make-bed \
            --out ~{agd_prefix_rename}

        plink \
            --bfile ~{agd_prefix_rename} \
            --bmerge ~{TGP_prefix} \
            --make-bed \
            --out merged_beds_files

        plink \
            --bfile ~{agd_prefix_rename} \
            --exclude merged_beds_files-merge.missnp \
            --make-bed \
            --out ~{agd_prefix_2}

        plink \
            --bfile ~{TGP_prefix} \
            --exclude merged_beds_files-merge.missnp \
            --make-bed \
            --out ~{TGP_prefix_2}

        plink \
            --bfile ~{agd_prefix_2} \
            --bmerge ~{TGP_prefix_2} \
            --make-bed \
            --out merged_beds_files2
        
        plink2 \
            --bfile merged_beds_files2 \
            --make-pgen \
            --out ~{out_string}
    }

    output{
        File out_pgen_file = out_string + ".pgen"
        File out_pvar_file = out_string + ".pvar"
        File out_psam_file = out_string + ".psam"
    }
}