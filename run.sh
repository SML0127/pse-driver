
#"amazon_oakley_us_new.json" 
#scripts=("ebay_gpu.json")
#scripts=("amazon_smith_helmet_us_new.json" "amazon_giro_us_new.json" "amazon_sweet_protection_us_new.json")
#scripts=( "amazon_smith_helmet_de_new.json" "amazon_sweet_protection_de_new.json")
#scripts=( "amazon_new_giro_us.json" "amazon_new_uvex_de.json" "amazon_new_poc_de.json" "amazon_new_smith_de.json" "amazon_new_smith_optics_us.json" "amazon_new_oak_de.json" "amazon_new_oakley_us.json" "amazon_new_giro_de.json" "amazon_new_giro_us.json" "amazon_new_sweet_protection_us.json" "amazon_new_sweet_de.json")
#scripts=( "rakuten_golf_honma.json"  "rakuten_golf_pro.json"  "rakuten_golf_mizno.json" "rakuten_golf_yamaha.json" "rakuten_golf_royal.json"  "rakuten_golf_maru.json"  "rakuten_golf_ten.json" )
#scripts=( "hyperdrug.json")

#scripts=( "jomashop_backpack.json" "jomashop_clutch1.json" "jomashop_clutch2.json" "jomashop_clutch3.json" "jomashop_handbag.json" "jomashop_shopperbag1.json" "jomashop_shopperbag2.json" "jomashop_shoulder.json" "jomashop_others1.json" "jomashop_others2.json" "jomashop_shoes.json")

#scripts=("jomashop_backpack.json""jomashop_clutch1.json", "jomashop_clutch2.json", "jomashop_clutch3.json", "jomashop_handbag.json", "jomashop_shopperbag1.json", "jomashop_shopperbag2.json", "jomashop_shoulder.json", "jomashop_others1.json", "jomashop_others2.json", "jomashop_shoes.json")

#scripts=("rakuten_fishing_new.json")
#scripts=( "rakuten_sewing_machine.json" )
#scripts=( "rakuten_golf_tailor_made.json" "rakuten_golf_titleist.json" "rakuten_golf_ping.json" "rakuten_golf_cleveland.json" "rakuten_golf_bridge.json" "rakuten_golf_honma.json"  "rakuten_golf_pro.json"  "rakuten_golf_mizno.json" "rakuten_golf_yamaha.json" "rakuten_golf_royal.json"  "rakuten_golf_maru.json" "rakuten_golf_roma.json"   "rakuten_fisihing_new.json")
#scripts=( "rakuten_golf_royal.json"  "rakuten_golf_maru.json" "rakuten_golf_roma.json")
scripts=( "jomashop_man_watch_new.json" "jomashop_backpack_new.json" "jomashop_clutch1_new.json" "jomashop_clutch2_new.json" "jomashop_clutch3_new.json" "jomashop_handbag_new.json" "jomashop_shopper1_new.json" "jomashop_shopper2_new.json" "jomashop_shoulder_new.json" "jomashop_others1_new.json" "jomashop_others2_new.json" "jomashop_shoes_new.json") 
#scripts=("jomashop_woman_watch_new.json" "jomashop_man_watch_new.json" "jomashop_backpack_new.json" "jomashop_clutch1_new.json" "jomashop_clutch2_new.json" "jomashop_clutch3_new.json" "jomashop_handbag_new.json" "jomashop_shopper1_new.json" "jomashop_shopper2_new.json" "jomashop_shoulder_new.json" "jomashop_others1_new.json" "jomashop_others2_new.json" "jomashop_shoes_new.json") 
for script in ${scripts[@]} 
do 
	echo 'python pse_driver.py --c run_from_file --wf ./script_pse/'$script' --ct ./script_category/zalando.json --cno 1'
	python pse_driver.py --c run_from_file --wf ./script_pse/$script --ct ./script_category/zalando.json --cno 1 
	sleep 10
done;


