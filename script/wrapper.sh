#!/bin/bash

# Create output directories
mkdir -p log/ out/

# Dump all settings to a yml file
echo "bids_app:" >> settings.yml
echo "  bids_dir: \"${bidsFolder}\"" >> settings.yml
echo "  participant_label: \"${subjectList}\"" >> settings.yml
echo "  parallel_npart: ${parallelParticipants}" >> settings.yml
echo "  executable: \"${execPath}\"" >> settings.yml
echo "  output_dir: \"out/\"" >> settings.yml
echo "  log_dir: \"log/\"" >> settings.yml
echo "  group_level: ${execGroupLevel}" >> settings.yml
echo "" >> settings.yml
echo "agave:" >> settings.yml
echo "  app_id: \"${AGAVE_APP_ID}\"" >> settings.yml
echo "  job_id: \"${AGAVE_JOB_ID}\"" >> settings.yml
echo "  job_name: \"${AGAVE_JOB_NAME}\"" >> settings.yml
echo "  execution_system: \"${AGAVE_JOB_EXECUTION_SYSTEM}\"" >> settings.yml
echo "  partition: \"${AGAVE_JOB_BATCH_QUEUE}\"" >> settings.yml
echo "  nodes: ${AGAVE_JOB_NODE_COUNT}" >> settings.yml
echo "  memory_per_node: ${AGAVE_JOB_MEMORY_PER_NODE}" >> settings.yml
echo "  cpu_per_node: ${AGAVE_JOB_PROCESSORS_PER_NODE}" >> settings.yml
echo "  max_runtime: \"${AGAVE_JOB_MAX_RUNTIME}\"" >> settings.yml
echo "  archive: ${AGAVE_JOB_ARCHIVE}" >> settings.yml
echo "  archive_system: \"${AGAVE_JOB_ARCHIVE_SYSTEM}\"" >> settings.yml
echo "  archive_url: \"${AGAVE_JOB_ARCHIVE_URL}\"" >> settings.yml
echo "  archive_path: \"${AGAVE_JOB_ARCHIVE_PATH}\"" >> settings.yml
echo "  owner: \"${AGAVE_JOB_OWNER}\"" >> settings.yml
echo "  submit_time: \"${AGAVE_JOB_SUBMIT_TIME}\"" >> settings.yml
echo "  tenant: \"${AGAVE_JOB_TENANT}\"" >> settings.yml

agave_wrapper settings.yml
wrapper_code=$?

${AGAVE_JOB_CALLBACK_CLEANING_UP}
rm -rf ${bidsFolder}
rm -rf work/

if [[ "${wrapper_code}" -gt "0" ]]; then
	${AGAVE_JOB_CALLBACK_FAILURE}
fi