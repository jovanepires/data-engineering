echo ">>> running stg pipeline"
python -m dags.stg_pipeline

echo ">>> running dw pipeline"
python -m dags.dw_pipeline