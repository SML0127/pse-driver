import traceback
from managers.graph_manager import GraphManager
from managers.settings_manager import *
from price_parser import Price
import re

class Exporter():

  def init(self):
    self.setting_manager = SettingsManager()
    self.setting_manager.setting("/home/pse/pse-driver/settings-worker.yaml")
    settings = self.setting_manager.get_settings()
    self.graph_mgr = GraphManager()
    self.graph_mgr.init(settings)


  def export_from_selected_mpid(self, job_id, exec_id, mpid):
    graph_mgr = self.graph_mgr
   
    #exchange_rate float, tariff_rate float, vat_rate float, tariff_threshold float, margin_rate float, min_margin float, delivery_company varchar(2048), shipping_cost float)
    pricing_information = graph_mgr.get_pricing_information(job_id)
    smpid = graph_mgr.get_smpid_from_job_id(job_id)
    cnum  = graph_mgr.get_cnum_from_job_configuration(job_id)
    selected_pricing_information = graph_mgr.get_selected_pricing_information(job_id, mpid)
    pricing_information['margin_rate'] = selected_pricing_information['margin_rate']
    pricing_information['min_margin'] = selected_pricing_information['min_margin']
    pricing_information['shipping_cost'] = selected_pricing_information['shpping_cost']
    pricing_information['min_price'] = selected_pricing_information['min_price']
    node_properties = graph_mgr.get_node_properties_from_mysite(exec_id, mpid)
    node_properties['pricing_information'] = pricing_information
    node_properties['smpid'] = smpid
    node_properties['cnum'] = cnum
    print(smpid)
    
    return user_defined_export(graph_mgr, mpid, node_properties), node_properties



  def export_from_mpid(self, job_id, exec_id, mpid):
    graph_mgr = self.graph_mgr
   
    smpid = graph_mgr.get_smpid_from_job_id(job_id)
    cnum  = graph_mgr.get_cnum_from_job_configuration(job_id)
    pricing_information = graph_mgr.get_pricing_information(job_id)
    node_properties = graph_mgr.get_node_properties_from_mysite(exec_id, mpid)
    node_properties['pricing_information'] = pricing_information
    delivery_company = pricing_information['delivery_company']
    shpping_fee = graph_mgr.get_shipping_fee(delivery_company)
    node_properties['smpid'] = smpid
    node_properties['cnum'] = cnum
    node_properties['shipping_fee'] = shpping_fee
    
    return user_defined_export(graph_mgr, mpid, node_properties), node_properties

  def export(self, node_id):
    #print(self.graph_mgr.get_node_properties(node_id))
    graph_mgr = self.graph_mgr
    node_properties = graph_mgr.get_node_properties(node_id)

    return user_defined_export(graph_mgr, node_id, node_properties)
    #exec('print(graph_mgr.get_node_properties(node_id))')
  def import_rules(self):
    exec('def get_price(graph_mgr, node_id, node_properties): print(node_id); return node_properties["price"]', globals())
  
  def import_rules_from_file(self, name):
    f = open(name, 'r')
    exec(f.read(), globals(), globals())
    f.close()
  
  def import_rules_from_code(self, code):
    exec(code, globals(), globals())

  def export_with_rules(self):
    self.export()

  def close(self):
    self.graph_mgr.disconnect()



if __name__ == '__main__':

  exporter = Exporter()
  exporter.init()
  try:
    #node_ids = exporter.graph_mgr.find_nodes_of_execution_with_label(649, 7)
    print(exporter.graph_mgr.get_pricing_information(183))
    #exporter.import_rules_from_file('./cafe24_amazon.py')
   # exporter.import_rules_from_file('./cafe24_zalando.py')
    #exporter.export(node_ids[0])
  except:
    print(str(traceback.format_exc()))
    pass
  exporter.close()

