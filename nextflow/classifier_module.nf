nextflow.enable.dsl = 2


params.publish_all_classifer_cands = true
params.out_dir = "${params.search_dir}/${params.obsid}_candidates"

process feature_extract {
    label 'cpu'
    label 'lofar_feature_lab'

    time '1h'
    errorStrategy 'retry'
    maxRetries 1

    input:
    path pfd_files

    output:
    path "*.arff"
    path "*pfd*", includeInputs: true

    """
    ls
    cat `ls ./ | head -n 1`
    python `which PulsarFeatureLab.py` -d `pwd` -f feature_extraction.arff -t 6 -c 3 --meta --arff
    """

}

process classify {
    label 'lofar_ml'

    input:
    path fex_out
    path pfd_files

    output:
    path "feature_extraction*"
    path "*pfd*", includeInputs: true

    """
    REALPATH=`realpath ${fex_out}`
    for i in {1..5}; do
        java -jar `which LOTAASClassifier.jar` -m \${LOTAAS_MLC_MODEL_DIR}/V1.3.1model\${i}.model -p `realpath ${fex_out}` -a 1 -d
        if [ -f "\${REALPATH%arff}positive" ]; then
            mv \${REALPATH%arff}positive feature_extraction_m\${i}.positive
        fi
        if [ -f "\${REALPATH%arff}negative" ]; then
            mv \${REALPATH%arff}negative feature_extraction_m\${i}.negative
        fi
    done
    """
}

process sort_detections {
    label 'lofar_ml'

    publishDir params.out_dir, mode: 'copy', enabled: params.publish_all_classifer_cands

    input:
    path classifier_files
    path pfd_files

    output:
    path "positive_detections/*" optional true
    path "negative_detections/*" optional true

    """
    LOTAAS_wrapper.py
    if [ -f LOTAAS_positive_detections.txt ]; then
        mkdir positive_detections
        for i in \$(cat LOTAAS_positive_detections.txt); do
            mv \${i}* positive_detections
        done
    fi
    if [ -f LOTAAS_negative_detections.txt ]; then
        mkdir negative_detections
        for i in \$(cat LOTAAS_negative_detections.txt); do
            mv \${i}* negative_detections
        done
    fi
    """
}


workflow classifier {
    take:
        pfd_files
    main:
        feature_extract( pfd_files )
        classify( feature_extract.out[0],\
                  feature_extract.out[1] )
        sort_detections( classify.out[0],\
                         classify.out[1] )//pfd_files )
    emit:
        sort_detections.out[0]
        sort_detections.out[1]
}
