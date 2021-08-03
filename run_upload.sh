
#for script in 390 391 392 393 394 395 396 397 398 399 400 401 402 403 404 408 409 410 411 412 413 414
#for script in 408 412
#568
#for script in  718 726 728 729 730 731 733 734
for script in  999 1000 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 1011
#for script in 951 952 953 954 955 956 957 958 959 960 961 
do
	echo 'python cafe24_driver.py --cafe24_c run --cafe24_eid '$script' --cafe24_label 3 --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue --cafe24_mall script_cafe24/mallmalljmjm0.json --cafe24_code script_transformation/eid_'$script'.py'
	python cafe24_driver.py --cafe24_c run --cafe24_eid $script --cafe24_label 3 --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue --cafe24_mall script_cafe24/mallmalljmjm0.json --cafe24_code script_transformation/eid_$script.py
	#python cafe24_driver.py --cafe24_c run --cafe24_eid $script --cafe24_label 3 --cafe24_host 127.0.0.1 --cafe24_port 6379 --cafe24_queue cafe24_queue --cafe24_mall script_cafe24/mallmalljmjm0.json --cafe24_code script_transformation/eid_$script.py --url https://www.mallmalljmjm.cafe24.com
done;




