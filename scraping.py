import requests
from bs4 import BeautifulSoup
from astropy import units as u
from astropy.coordinates import SkyCoord

URL = "https://theskylive.com/quickaccess?objects=moon&data=equatorial-time"
page = requests.get(URL)

soup = BeautifulSoup(page.content, "html.parser")

results = soup.find_all("div", class_="keyinfobox")


i=0
for job_element in results:
    i+=1
    if i == 4:
        declination = job_element.find("ar").text
    if i == 5:
        LMST = job_element.find("ar").text
print(declination, LMST)
LMST_format = LMST.replace('°', '').replace('’', '').replace('”', '')
c = SkyCoord(declination + ' ' + LMST_format, unit=(u.hourangle, u.deg))
print(c)