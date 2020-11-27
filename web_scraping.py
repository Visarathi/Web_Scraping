import requests
from bs4 import BeautifulSoup
import pandas
import argparse
import connect

parser = argparse.ArgumentParser()
parser.add_argument("--page_num_MAX", help="enter the number of pages to parser",type=int)
parser.add_argument("--dbname", help="enter the name of db",type=str)
args = parser.parse_args()

oyo_url = "https://www.oyorooms.com/hotels-in-bangalore/?page="
page_num_MAX = args.page_num_MAX
scraped_info_list = []
connect.connect(args.dbname)

for page_num in range(1,page_num_MAX):
    url = oyo_url + str(page_num)
    print("GET Requests for:" + url)
    req = requests.get(url)
    content = req.content
    
    soup = BeautifulSoup(content,"html.parser")
    
    all_hotels = soup.find_all("div", {"class":"hotelCardListing"})
    
    for hotel in all_hotels:
        hotel_dict = {}
        hotel_dict["name"] = hotel.find("h3",{"class":"listingHotelDescription__hotelName"}).text
        hotel_dict["address"] = hotel.find("span",{"iteamprop":"streetAddress"}).text
        hotel_dict["price"] = hotel.find("span",{"class":"listingPrice__finalPrice"}).text
        
        try:
            hotel_dict["rating"] = hotel.find("span",{"class":"hotelRating__ratingSummary"}).text
        except AttributeError:
            hotel_dict["rating"] = None
            
        parent_amenities_element = hotel.find("div",{"class":"amenityWrapper"})
        
        amenities_list = []
        for amenity in parent_amenities_element.find_all("div",{"class":"amenityWrapper"}):
            amenities_list.append(amenity.find("span",{"class":"d-body-sm"}).text.strip())
            
        hotel_dict["amenities"] = ', '.join(amenities_list[:-1])
        scraped_info_list.append(hotel_dict)
        connect.insert_into_table(args.dbname, tuple(hotel_dict.values()))
    
dataFrame = pandas.DataFrame(scraped_info_list)
print("Creating csv file...")
dataFrame.to_csv("Oyo.csv")
connect.get_hotel_info(agrs.dbname)



#storging the in sql database using sqlite3  
import sqlite3

def connect(dbname):
    
    conn = sqlite3.connect(dbname)
    conn.execute("CREATE TABLE IF NOT EXISTS OYO_HOTELS(NAME TEXT, ADDRESS TEXT, PRICE INT, AMENITIES TEXT, RATING TEXT)")
    print("Table created sucessfully")
    conn.close()


def insert_into_table(dbname, values):
    
    conn = sqlite3.connect(dbname)
    print("Insert into table:"+ str(values))  
    insert_sql = "INSERT INTO OYO_HOTELS(NAME, ADDRESS, PRICE, AMENITIES, RATING) values(?, ?, ?, ?, ?)"
    conn.execute(insert_sql, values)
    conn.commit()
    conn.close()
    

def get_hotel_info(dbname):
    
    conn = sqlite3.connect(dbname)
    cur = conn.coursor()
    cur.execute("SELECT * FROM OYO_HOTELS")
    table_data = cur.fetchall()
    
    for record in table_data:
        print(record)
    conn.close()


