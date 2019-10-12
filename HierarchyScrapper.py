from lxml import html
import requests
import re
import time
import csv
from selenium import webdriver

#'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/variable.cgi?study_id=phs000209.v13.p3&phv=82624&phd=1712&pha=&pht=1116&phvf=1&phdf=&phaf=&phtf=3&dssp=1&consent=&temp=1'
def getNextNode(browser,url,path_with_variable):
    browser.get(url)
    time.sleep(.5)
    #variables = browser.find_elements_by_xpath('.//*[@id="associatedVariables"]/div')[0]
    #print("variables" + variables)
    #print('\\' + variables.text + '\\')
    inner_nodes = browser.find_elements_by_xpath('.//*[@id="associatedVariables"]//li[contains(@style,"page-foldericon")]/div')
    value_nodes = browser.find_elements_by_xpath('.//div[@id="associatedVariables"]//a[contains(@onclick,"variable.cgi")]')

    if(len(value_nodes) > 0):
        scrapeValueNodes(browser,value_nodes)
    
    if(len(inner_nodes) > 0):
        found_phvf = dict()
        
        for inner_node in inner_nodes:
            #div_node = inner_node.find_element_by_xpath('.//*[@class="groupNode"]')
            collectPhvf(inner_node, found_phvf)
            #phvf = inner_node.get_attribute("onclick")
            #phvf = phvf.replace("setState('phvf', ","")
            #phvf = re.sub(r'\).*','',phvf)
            #print(phvf)
            #url = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/variable.cgi?study_id=' + study_id + '&phv=' + phv
            #nexturl = url + '&phvf=' + phvf
            #print(nexturl)
            #browser.get(nexturl)
            
            #getNextNode(browser,nexturl,pathWithVariable)
                
        for node, phvf in found_phvf.items():
            url = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/variable.cgi?study_id=' + study_id + '&phv=' + phv
            nexturl = url + '&phvf=' + phvf
            getNextNode(browser,nexturl,path_with_variable)
            browser.get(nexturl)
                    
def collectPhvf(inner_node, tdict):
    phvf = inner_node.get_attribute("onclick")
    phvf = phvf.replace("setState('phvf', ","")
    phvf = re.sub(r'\).*','',phvf)
    tdict[inner_node.text] = phvf
    
    return dict
    
def scrapeValueNodes(browser,value_nodes):
    try:
        with open('./data/' + study_id + '.hierarchy.csv','a') as csv_file:
            writer = csv.writer(csv_file)
            
            concept_dict = dict();    
            for node in value_nodes:
                
                path = ''
        
                root_node = browser.find_element_by_xpath('.//*[@class="studyNode"]').text
                path = path + '\\' + root_node + '\\'
                
                subpaths = browser.find_elements_by_xpath('.//div[@id="associatedVariables"]//div[@class="groupNode"]')
                for spath in subpaths:
                    
                    path = path + spath.text + '\\'
        
                concept_dict[node.text] = path
            
            for k, v in concept_dict.items():
                print(k)
                print(v)
                writer.writerow([k,v])
                
    except (Exception):
        pass
       
study_id = 'phs000209.v13.p3'
phv = '82624'
url = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/variable.cgi?study_id=' + study_id + '&phv=' + phv

browser = webdriver.Chrome()
browser.get(url)

time.sleep(1)

#avarselem = browser.find_elements_by_xpath("//div[@id=associatedVariables]/div/ul")

element = browser.page_source


groupNodes = browser.find_elements_by_xpath('.//*[@class="groupNode"]')
 
nodes_with_phvf = dict()

path_with_variable = dict()

for groupNode in groupNodes:
    
    phvf = groupNode.get_attribute("onclick")
    phvf = phvf.replace("setState('phvf', ","")
    phvf = re.sub(r'\).*','',phvf)
    
    nodes_with_phvf[groupNode.text] = phvf
    
for node, phvf in nodes_with_phvf.items():
    nexturl = url + '&phvf=' + phvf
    getNextNode(browser,nexturl,path_with_variable)
    #browser.get(nexturl)

print(list(path_with_variable.items()))

print('script done')