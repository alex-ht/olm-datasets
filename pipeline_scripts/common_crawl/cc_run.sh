#!/bin/bash
set -ex
NUM_PROC=$(nproc --all)
SNAPSHOTS="CC-MAIN-2022-49 CC-MAIN-2022-40 CC-MAIN-2022-33 CC-MAIN-2022-27 CC-MAIN-2022-21 \
           CC-MAIN-2022-05 CC-MAIN-2021-49 CC-MAIN-2021-43 CC-MAIN-2021-39 CC-MAIN-2021-31 \
           CC-MAIN-2021-25 CC-MAIN-2021-21 CC-MAIN-2021-17 CC-MAIN-2021-10 CC-MAIN-2021-04"
SAMPLING_RATIOS=$(printf '0.01 %.0s' {1..15})

python download_common_crawl.py \
  --snapshots $SNAPSHOTS \
  --segment_sampling_ratios $SAMPLING_RATIOS \
  --seed=42 \
  --download_dir=common_crawl_wet_downloads \
  --num_proc=$NUM_PROC

python get_text_dataset_from_wet_downloads.py \
  --download_dir=common_crawl_wet_downloads \
  --output_dataset_name=cc_raw \
  --num_proc=$NUM_PROC

SPLITS="te kn ne as ml or gu mr"

for SPLIT in $SPLITS; do
  python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia/$SPLIT --url_column=url --split=$SPLIT --num_proc=$NUM_PROC
  ulimit -Sn 1000000 && python deduplicate.py \
     --input_dataset_name=cc_no_wikipedia/$SPLIT \
     --output_dataset_name=cc_olm/$SPLIT \
     --text_column=text \
     --remove_whole_example \
     --num_proc=$NUM_PROC
done

SPLITS="zh vi es ur ar hi pt en id eu bn ca fr"
for SPLIT in $SPLITS; do
  python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia/$SPLIT --url_column=url --split=$SPLIT --num_proc=$NUM_PROC
  python apply_bigscience_filters.py \
     --input_dataset_name=cc_no_wikipedia/$SPLIT \
     --output_dataset_name=cc_filtered/$SPLIT \
     --lang_id=$SPLIT --text_column=text --num_proc=$NUM_PROC
  ulimit -Sn 1000000 && python deduplicate.py \
     --input_dataset_name=cc_filtered/$SPLIT \
     --output_dataset_name=cc_olm/$SPLIT \
     --text_column=text \
     --remove_whole_example \
     --num_proc=$NUM_PROC
done
