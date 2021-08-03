
#run driver for crawling (e,g, mallmalljmjm0.json)
python pse_driver.py --c run_from_file --wf ./script_pse/$script --ct ./script_category/zalando.json --cno 1 

#run driver for upload (e,g, $script = 851)
python cafe24_driver.py --cafe24_c run --cafe24_eid $script --cafe24_label 3 --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue --cafe24_mall script_cafe24/mallmalljmjm0.json --cafe24_code script_transformation/eid_$script.py

# run driver for transform to mysite
python pse_driver.py --c transform_to_mysite --job_id $script --groupbykey name description price spid

# run driver for update mysite
python pse_driver.py --c update_to_mysite --job_id 3 --groupbykey name description price spid



# run worker for crawling
rq worker --url redis://141.223.197.33:63790 -w engine.pse_worker.pseWorker --job-class engine.pse_job.pseJob real_queue

# run worke for upload
rq worker --url redis://141.223.197.33:63790 -w plugin.cafe24.cafe24_uploader.Cafe24Uploader --job-class plugin.cafe24.upload_job.uploadJob cafe24_queue

