import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import multiprocessing
from multiprocessing import Process
import datetime
import os
import subprocess
import time
import random
import undetected_chromedriver as uc
import sqlite3
from threading import Thread
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

today = datetime.datetime.now()
today_month = str(today.month)
today_day = str(today.day)
if len(today_month) == 1: today_month = '0' + today_month
if len(today_day) == 1: today_day = '0' + today_day

#con = sqlite3.connect("mtst.db")
#cur = con.cursor()
conv_categories = {}
rate_categories = {}
hotel_itt = 1
conv_itt = 1
rate_itt = 1

info_d = multiprocessing.Queue()
conv_category_d = multiprocessing.Queue()
convenience_d = multiprocessing.Queue()
rate_category_d = multiprocessing.Queue()
rating_d = multiprocessing.Queue()
comments_d = multiprocessing.Queue()

today = today_day + today_month + str(today.year)
print(today)

def start_driver():
    global driver, opts

    opts = uc.ChromeOptions()
    opts.add_argument("--blink-settings=imagesEnabled=false")
    #opts.add_argument("User-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecho) Chrome/108.0.0.0 Safari/537.36")
    #opts.add_argument("Cookie=qrator_jsr=1701098640.868.Jg4vm0DIlnfUA0JA-psfmhdqpeemdpqh9mm318ikilai589ij-00")
    #opts.add_argument("--headless")
    #opts.add_argument("--no-sandbox")
    #opts.add_argument("--disable-deb-shm-usage")
    #opts.binary_location = "/opt/google/chrome/chrome"
    #chrome_driver = "/Work/uniontest/chromedriver"
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver = uc.Chrome(options=opts)

logfile = open("log.txt", 'w')
start_driver()
page_counter = 0

def get_hotel_info(browser, data):
    global cur, con, conv_categories, rate_categories, rate_itt, conv_itt
    global info_d, conv_category_d, convenience_d, rate_category_d, rating_d, comments_d

    hotelID = data[0]
    link = data[1]
    name = data[2]
    star = data[3]

    connect = False
    while not connect:
        try:
            browser.get(link)
            connect = True
        except Exception:
           time.sleep(1) 

    while browser.execute_script("return document.readyState;") != "complete":
        pass

    items_start = []
    while items_start == []:
        items_start = browser.find_elements(By.CLASS_NAME, "items-start")

    try:
        rate = items_start[0].find_element(By.CLASS_NAME, "text-white").get_attribute("innerHTML")
    except Exception:
        get_hotel_info(browser, data)
        return

    try:
        location = items_start[0].find_elements(By.CLASS_NAME, "-ml-2\.5")[0].find_elements(By.TAG_NAME, "span")[1].get_attribute("innerHTML").split(", ")
    except Exception:
        get_hotel_info(browser, data)
        return

    city = location[0]
    place = ", ".join(location[1::])
    
    try:
        description = browser.find_element(By.CLASS_NAME, "line-clamp-3").get_attribute("innerHTML")
    except Exception:
        get_hotel_info(browser, data)
        return 

    for_info = [city, place, name, link, rate, star, description]
    
    try:
        sections = browser.find_elements(By.TAG_NAME, "section")
        comments_main = sections[2].find_elements(By.TAG_NAME, "li")
    except Exception:
        return

    comments_list = []
    if len(comments_main) == 0:
        comments_main = sections[2].find_elements(By.CLASS_NAME, "line-clamp-3")

    if len(comments_main) != 0:
        comms_data = [name, None] * len(comments_main)
    
        sql_ins_comms = "insert into comments values " + "((select ID from info where hotel_name=?), ?), " * len(comments_main)
        sql_ins_comms = sql_ins_comms[:-2]
        for i in range(len(comments_main)):
            comms_data[2 * i + 1] = comments_main[i].get_attribute("innerHTML")

        comments_list.append([sql_ins_comms, comms_data])

    try:
        dr_block = browser.find_element(By.CLASS_NAME, "gap-x-10")
        detailed_rating = {}
        sql_ins_rating = "insert into rating values "
        sql_ins_data = []
        for dr_section in dr_block.find_elements(By.CLASS_NAME, "mb-2"):
            rate_info = dr_section.find_elements(By.TAG_NAME, "p")

            rate_info[0] = rate_info[0].get_attribute("innerHTML")
            rate_info[1] = rate_info[1].get_attribute("innerHTML")
    
            detailed_rating[rate_info[0]] = rate_info[1]
    
            rate_category_d.put([rate_info[0]])
        
            sql_ins_rating += "((select ID from info where hotel_name=?), (select ID from rate_category where name=?), ?), "
            sql_ins_data.append(name)
            sql_ins_data.append(rate_info[0])
            sql_ins_data.append(rate_info[1])
    except Exception:
        get_hotel_info(browser, data)
        return

    sql_ins_rating = sql_ins_rating[:-2]

    try:
        services_blocks = browser.find_element(By.ID, "hotel-services").find_elements(By.CLASS_NAME, "gap-y-6")
    except Exception:
        get_hotel_info(browser, data)
        return

    info_d.put(for_info)

    services = {}
    for serv_block in services_blocks:
        for serv_category in serv_block.find_elements(By.CLASS_NAME, "p-4"):
            srv_data = serv_category.find_elements(By.TAG_NAME, "p")
            for i in range(len(srv_data)):
                srv_data[i] = srv_data[i].get_attribute("innerHTML")
            cur_services = srv_data[1::]

#            sql_ins_conv_categ = "insert into conv_category(name) values(?)"
            conv_category_d.put([srv_data[0]])
            sql_ins_conv = "insert into convenience(hotelID, conv_categoryID, value) values"

            q_data = []
            sql_hotel_ID = "(select ID from info where hotel_name=?)"
            sql_category_ID = "(select ID from conv_category where name=?)"
            sql_ins_row = "(" + sql_hotel_ID + ", " + sql_category_ID + ", ?)"
            for i in range(len(cur_services)):
                sql_ins_conv += sql_ins_row + ", "
                q_data.append(name)
                q_data.append(srv_data[0])
                q_data.append(cur_services[i])

            sql_ins_conv = sql_ins_conv[:-2]
#            print([q_data])
            convenience_d.put([sql_ins_conv, q_data])

    rating_d.put([sql_ins_rating, sql_ins_data])
    if (comments_list) != 0:
        for d in comments_list:
            comments_d.put(d)

    print("Done hotel parse")

def multiparser(hotels):
    print("I am multiparser.")

    opts = uc.ChromeOptions()
    opts.add_argument("--blink-settings=imagesEnabled=false")
    loc_driver = None
    while loc_driver == None:
        try:
            loc_driver = uc.Chrome(options=opts)
        except Exception:
            pass

    #PROGRAMWORK = program_work_q.get()

    #print("today dir creation")
    #os.system("mkdir ./storage/dns" + today)
    #os.system("mkdir ./storage/dns" + today + "/notmove")

    while(True):
        data = hotels.get()
#        print(hotel_link)
        if data[0] == "Done":
            break
        print(data)
        get_hotel_info(loc_driver, data)

    info_d.put(["Done"])
    conv_category_d.put(["Done"])
    convenience_d.put(["Done"])
    rating_d.put(["Done"])
    rate_category_d.put(["Done"])
    comments_d.put(["Done"])

    loc_driver.close()
    loc_driver.quit()

def get_global_info(country_url):
    global driver, site, hotel_q, info_d, convenience_d, category_d
    global opts

    hotelID = 1
    hotel_count = None
    read_count = 0
    page = 1
    max_page = 1
    count_empty_pages = 0
    all_hotels = {}
#    ignore_exceptions=(NoSuchElementException, StaleElementReferenceException,)

    while (hotel_count == None or read_count < hotel_count):
        go_link = site + "/s/?page=" + str(page) + "&" + country_url
        print(go_link)
        driver.get(go_link)

#        WebDriverWait(driver, 10).until(lambda driver: driver.execute_script("return document.readyState") == "complete")
#        print(driver.execute_script("return document.readyState"))

        connect = False
        while not connect:        
            try:
                element_present = EC.presence_of_element_located((By.CLASS_NAME, "text-left"))
                WebDriverWait(driver, 20).until(element_present)
                connect = True
            except Exception:
#                driver.quit()
#                driver = uc.Chrome(options=opts)
                driver.get(go_link)
        
        while hotel_count == None:
            count_info = driver.find_element(By.CLASS_NAME, "text-left")
            count_info = count_info.find_element(By.TAG_NAME, "div").get_attribute("innerHTML")
            count_info_splitted = count_info.split(" ")

            if count_info_splitted[0] == "Готово!":
                hotel_count = int(count_info.split(" ")[2])
                print(hotel_count)
       
#        condition = EC.presence_of_all_elements_located((By.CLASS_NAME, "isolate"))
#        hotels = WebDriverWait(driver, 60).until(condition)
        hotels = driver.find_elements(By.CLASS_NAME, "isolate")
        data_from_page = []
        BadSituation = False

        if len(hotels) == 0:
            if count_empty_pages >= 9:
                break
            count_empty_pages += 1
            continue
        else:
            count_empty_pages = 0

        for hotel in hotels:
            try:
                a_teg = hotel.find_element(By.TAG_NAME, "a")
            except Exception:
                print("a_teg retry")
                BadSituation = True
                page -= 1
                read_count -= len(hotels)
                break
       
            try:
                link = a_teg.get_attribute("href")
            except Exception:
                print("link retry")
                BadSituation = True
                page -= 1
                read_count -= len(hotels)
                break

            try:
                name = hotel.find_element(By.CLASS_NAME, "select-all").get_attribute("innerHTML")
                control_point = True
            except Exception:
                BadSituation = True
                print("name retry")
                page -= 1
                read_count -= len(hotels)
                break
            
            star = None
            try:
                star = hotel.find_element(By.CLASS_NAME, "inline-flex")
                star = int(star.find_element(By.TAG_NAME, "span").get_attribute("innerHTML"))
            except:
                pass
            
            data = [hotelID, link, name, star]    
            hotelID += 1
            data_from_page.append(data)
            
            print(str(star) + "\t\t" + name)
            print()

        if not BadSituation:
            for data in data_from_page:
                hotel_q.put(data)

        page += 1
        read_count += len(hotels)

def filldb(info, conv_categ, convenience, rate_categ, rating, comments):
    con = sqlite3.connect("mtst.db")
    cur = con.cursor()
    done_count = [0, 0, 0, 0, 0, 0] 
    count_of_info = 0

    while True:
        print(done_count)

        if done_count[0] < 3 or not info.empty():
            info_data = info.get()
            
            if info_data[0] == "Done":
                done_count[0] += 1
            else:
                print("writing info..")
                sql_ins_info = """insert into info(city, address, hotel_name, link_reservation, global_rate, star, description)
                                values(?, ?, ?, ?, ?, ?, ?)"""
   #              print(sql_ins_info)
                cur.execute(sql_ins_info, info_data)
                con.commit()
                count_of_info += 1

        if done_count[1] < 3 or not conv_categ.empty():
            conv_categ_data = conv_categ.get()

            if conv_categ_data[0] == "Done":
                done_count[1] += 1
            else:
                print("writing conv_categories...")
                sql_ins_conv_categ = "insert into conv_category(name) values(?)"
                try:
                    cur.execute(sql_ins_conv_categ, conv_categ_data)
                    con.commit()
                except sqlite3.IntegrityError:
                    con.commit()

        if done_count[2] < 3 or not convenience.empty():
            conv_q = convenience.get()

            if conv_q[0] == "Done":
                done_count[2] += 1
            else:
                print("writing conveniense...")
                try:
                    cur.execute(conv_q[0], conv_q[1])
                    con.commit()
                except sqlite3.IntegrityError:
                    con.commit()
                    convenience.put(conv_q)

        if done_count[3] < 3 or not comments.empty():
            comms_q = comments.get()
#            print(comms_q)

            if comms_q[0] == "Done":
                done_count[3] += 1
            else:
                print("writing comments...")
                try:
                    cur.execute(comms_q[0], comms_q[1])
                    con.commit()
                except sqlite3.IntegrityError:
                    con.commit()
                    convenience.put(comms_q)

        if done_count[4] < 3 or not rate_categ.empty():
            rate_categ_data = rate_categ.get()

            if rate_categ_data[0] == "Done":
                done_count[4] += 1
            else:
                print("writing rate_categories...")
                sql_ins_rate_categ = "insert into rate_category(name) values(?)"
                try:
                    cur.execute(sql_ins_rate_categ, rate_categ_data)
                    con.commit()
                except sqlite3.IntegrityError:
                    con.commit()
        
        if done_count[5] < 3 or not rating.empty():
            rating_q = rating.get()

            if rating_q[0] == "Done":
                done_count[5] += 1
            else:
                print("writing rating...")
                try:
                    cur.execute(rating_q[0], rating_q[1])
                    con.commit()
                except sqlite3.IntegrityError:
                    con.commit()
                    rating.put(rating_q)
 
        if sum(done_count) >= 18:
            break

    print("db is writed")
    print(count_of_info)

#program_work_q = multiprocessing.Queue()
#file_queue = multiprocessing.Queue()
#Process(target=write_loop, args=(file_queue, program_work_q)).start()

hotel_q = multiprocessing.Queue()
proc1 = Process(target=multiparser, args={hotel_q}).start()
proc2 = Process(target=multiparser, args={hotel_q}).start()
proc3 = Process(target=multiparser, args={hotel_q}).start()


#thread1 = Thread(target=insert_info, args=[info_d, con, cur])
#thread2 = Thread(target=insert_conv_category, args=[conv_category_d, con, cur])
#thread3 = Thread(target=insert_convenience, args=[convenience_d, con, cur])
#thread4 = Thread(target=insert_comments, args=[rate_category_d, con, cur])
#thread5 = Thread(target=insert_rate_category, args=[rating_d, con, cur])
#thread6 = Thread(target=insert_rating, args=[comments_d, con, cur])

#thread1.start()
#thread2.start()
#thread3.start()
#thread4.start()
#thread5.start()
#thread6.start()
db_proc = Process(target=filldb, args=[info_d, conv_category_d, convenience_d, rate_category_d, rating_d, comments_d]).start()

site = "https://travel.mts.ru"
#country = [ "title=%D0%93%D1%80%D1%83%D0%B7%D0%B8%D1%8F&location=7cda94f8-5cb7-4a88-8940-a73572b0bce3",
#            "title=%D0%A2%D1%83%D1%80%D1%86%D0%B8%D1%8F&location=fa6b36a2-75f0-45ae-9b23-a7cff96f2313",
#            "title=%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F&location=612da5c8-3938-48c9-969b-94a23feecf7a"]

country = ["title=%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F&location=612da5c8-3938-48c9-969b-94a23feecf7a"]

#country = [ "title=%D0%93%D1%80%D1%83%D0%B7%D0%B8%D1%8F&location=7cda94f8-5cb7-4a88-8940-a73572b0bce3"]

get_global_info(country[0])
#get_global_info(country[1])
#get_global_info(country[2])

hotel_q.put(["Done"])
hotel_q.put(["Done"])
hotel_q.put(["Done"])

driver.close()
driver.quit()

#proc4 = Process(target=multiparser, args={hotel_q}).start()
#driver.get(site)
#time.sleep(1)
#item = []
#while item == []:
#    item = driver.find_elements(By.CLASS_NAME, "isolate")
#print(item[0].get_attribute("innerHTML"))
#print(len(item))
# WebDriverWait(driver, 30).until(
# EC.presence_of_element_located((By.CLASS_NAME, "subcategory__item"))
# )

#category = "https://travel.mts.ru/s?page=1&title=%D0%93%D1%80%D1%83%D0%B7%D0%B8%D1%8F&location=7cda94f8-5cb7-4a88-8940-a73572b0bce3"
#print(recursive_parse_catalog(category, "subcategory__item-container ", "ui-link"))


#proc1.join()
print("\nGlobal Done\n")
os.system("sleep 200")
# logfile.write("\nDone\n")

#program_work_q.put(False)

# logfile.close()
#driver.close()
#driver.quit()
