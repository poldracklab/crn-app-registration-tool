#!/bin/bash

# Let agave replace inputs and parameters here
# since these are not variables, are replacements.
SUB_LIST="${subjectList}"
GROUP_SIZE="${groupSize}"
BIDS_FOLDER="${bidsFolder}"

# Start exit code variable and cwd
exit_code=0
CWD=$(pwd)

mkdir -p ${CWD}/log ${CWD}/out
LOG_FILE="${CWD}/log/logfile.txt"
ERR_FILE="${CWD}/log/errors.txt"
EXIT_FILE="${CWD}/log/exit_code.txt"

echo "Starting ${AGAVE_JOB_APP_ID} app" >> ${LOG_FILE}
echo "   bidsFolder=${BIDS_FOLDER}" >> ${LOG_FILE}
echo "   groupSize=${GROUP_SIZE}" >> ${LOG_FILE}
echo "   subjectList=${SUB_LIST}" >> ${LOG_FILE}

MEMORY=$( echo "${AGAVE_JOB_MEMORY_PER_NODE} * 1000" | bc -l )
RUNTIME_MAX_MIN=$( echo "${AGAVE_JOB_MAX_RUNTIME}" | awk -F: '{ print ($1 * 60) + $2 }' )
RUNTIME_MAX_CHILD=$( echo "${RUNTIME_MAX_MIN} - 10" | bc -l )
# Generate sbatch file
SBATCH_FILE=$CWD/${AGAVE_JOB_ID}.sbatch
echo "#!/bin/bash" >> ${SBATCH_FILE}
echo "#singularity.example" >> ${SBATCH_FILE}
echo "#SBATCH --nodes=${AGAVE_JOB_NODE_COUNT}" >> ${SBATCH_FILE}
echo "#SBATCH --time=${RUNTIME_MAX_CHILD}" >> ${SBATCH_FILE}
echo "#SBATCH --mincpus=${AGAVE_JOB_PROCESSORS_PER_NODE}" >> ${SBATCH_FILE}
echo "#SBATCH --mem-per-cpu=${MEMORY%.*}" >> ${SBATCH_FILE}
echo "#SBATCH --qos=${AGAVE_JOB_BATCH_QUEUE}" >> ${SBATCH_FILE}
echo "#SBATCH --partition=${AGAVE_JOB_BATCH_QUEUE}" >> ${SBATCH_FILE}
echo "#SBATCH --mail-type=ALL" >> ${SBATCH_FILE}
echo "#SBATCH --mail-user=crn.poldracklab@gmail.com" >> ${SBATCH_FILE}

# Check sanity of groupsize
if [[ "${GROUP_SIZE}" -lt 1 ]]; then
    GROUP_SIZE=4
fi

subjects=()
# Check if a subjects list has been passed. Populate the list.
if [[ -n "${SUB_LIST// }" ]]; then
    subjects=(${SUB_LIST})
    echo "Subject list is not empty, processing (${subjects[@]})" >> ${LOG_FILE}
else
    for i in $(find ${BIDS_FOLDER} -name sub-* -type d | shuf ); do
    subid=$(basename $i)
    subjects+=(${subid:4})
    done
    echo "Listing all subjects (${subjects[@]})" >> ${LOG_FILE}
fi

if [[ "${#subjects[@]}" -lt 1 ]]; then
    echo "Subject list is empty" >> ${ERR_FILE}
    echo "1" > ${EXIT_FILE}
    exit 1
fi

subjob_id=0
SLURM_JOB_IDS=()
for((i=0; i < ${#subjects[@]}; i+=GROUP_SIZE)); do
    part=( "${subjects[@]:i:GROUP_SIZE}" )
    echo "Sub-job ID = ${subjob_id}, subjects ${part[*]}" >> ${LOG_FILE}
    job_batch="${CWD}/log/job-$(printf '%03d' ${subjob_id})"
    cp ${SBATCH_FILE} ${job_batch}.sbatch
    
    echo "#SBATCH --job-name=sinex-$(printf '%03d' ${subjob_id})" >> ${job_batch}.sbatch
    echo "#SBATCH --output=\"${job_batch}.out\"" >> ${job_batch}.sbatch
    echo "#SBATCH --error=\"${job_batch}.err\"" >> ${job_batch}.sbatch
    echo "module load singularity" >> ${job_batch}.sbatch
    echo "srun /scratch/PI/russpold/singularity_images/bids_example-2016-08-19-d8ff0ac62239.img ${CWD}/${BIDS_FOLDER} ${CWD}/out participant --participant_label ${part[*]}" >> ${job_batch}.sbatch

    # Submit to slurm
    sbatch "${job_batch}.sbatch" >> ${job_batch}.slurm
    last_code=$?
    exit_code=$(( ${exit_code} + ${last_code/#-/} ))

    # Get job id
    slurm_id=$(sed -n -e '/^Submitted batch job/p' ${job_batch}.slurm | tr -s ' ' | cut -d ' ' -f 4 )
    SLURM_JOB_IDS+=("${slurm_id}")

    cat ${job_batch}.slurm >> ${LOG_FILE}
    ((subjob_id+=1))
done

if [[ "${exit_code}" -gt "0" ]]; then
    echo "ERROR submitting ${SBATCH_FILE} into slurm (code ${exit_code})." >> ${ERR_FILE}
    exit 1
fi

# Busy wait
NJOBS="${#SLURM_JOB_IDS[@]}"
echo "Starting busy wait on ${NJOBS} jobs: ${SLURM_JOB_IDS[*]}" >> ${LOG_FILE}
while true; do
    job_finished=0
    for job_id in "${SLURM_JOB_IDS[@]}"; do
        squeue_out=$( squeue -j "${job_id}" )
        if [[ "$?" -ne "0" ]]; then
            echo "Job ${job_id} not found in queue" >> ${LOG_FILE}
            ((job_finished+=1))
            continue
        fi
        status=$( echo "${squeue_out}" | sed 1d | tr -s ' ' | cut -d ' ' -f 6 )
        echo "Job ${job_id} status is ${status}" >> ${LOG_FILE}
    done
    if [[ "${job_finished}" -eq "${NJOBS}" ]]; then
        echo "All jobs finished" >> ${LOG_FILE}
        break
    fi
    sleep 10s
done

# Run reduce job
echo "Starting reduce job..." >> ${LOG_FILE}
participants=""
if [[ -n "${SUB_LIST// }" ]]; then
    participants="--participant_label ${SUB_LIST}"
fi
module load singularity
/scratch/PI/russpold/singularity_images/bids_example-2016-08-19-d8ff0ac62239.img ${CWD}/${BIDS_FOLDER} ${CWD}/out group ${participants} >> ${LOG_FILE}
echo "Finished reduce job..." >> ${LOG_FILE}

# Cleanup logs
# cat *.err >> ${ERR_FILE}
# cat *.out >> ${LOG_FILE}

# Wipe out anything but log/ and out/
for tmpfile in $( ls | grep -v '^log$' | grep -v '^out$' ); do
    rm -rf $tmpfile
    echo "Cleaned up: $tmpfile"
done

echo "${exit_code}" > ${EXIT_FILE}
