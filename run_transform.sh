#for script in {1..2} 
#do 
#	echo 'python pse_driver.py --c transform_to_mysite --job_id '$script''
#        python pse_driver.py --c transform_to_mysite --job_id $script --groupbykey name description price spid
#	sleep 5
#done;

for script in {1..1} 
do 
	echo 'python pse_driver.py --c transform_to_mysite --job_id '$script''
        python pse_driver.py --c transform_to_mysite --job_id $script --groupbykey name description price spid
	sleep 5
done;

#for script in {1..2} 
#do 
#	echo 'python pse_driver.py --c transform_to_mysite --job_id '$script''
#        python pse_driver.py --c transform_to_mysite --job_id $script --groupbykey name description price spid
#	sleep 5
#done;
#for script in {15..26} 
#do 
#	echo 'python pse_driver.py --c transform_to_mysite --job_id '$script''
#        python pse_driver.py --c transform_to_mysite --job_id $script --groupbykey name description price
#	sleep 5
#done;
#
