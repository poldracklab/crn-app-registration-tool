#!/bin/bash

# Create output directories
mkdir -p log/ out/

# Dump all settings to a yml file
echo "agave:" >> settings.yml
echo "  job_id: \"${AGAVE_JOB_ID}\"" >> settings.yml
echo "  app_id: \"${AGAVE_APP_ID}\"" >> settings.yml
echo "  memory_per_node: ${AGAVE_JOB_MEMORY_PER_NODE}" >> settings.yml
echo "  max_runtime: \"${AGAVE_JOB_MAX_RUNTIME}\"" >> settings.yml

agave_wrapper ${bidsFolder} out/ --participant_label ${subjectList} --group-size ${groupSize} --bids-app-name testapp --settings settings.yml
