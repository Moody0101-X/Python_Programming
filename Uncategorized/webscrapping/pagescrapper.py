"""
---------------------------------------------------------------------------------------------------------
scrap the whole pages of a website.
usage:
	vars: 
	pagenumber = 3
	url = "example.com"
website = webSiteScrapper(n, url)
print(website.run()) => prints a list of the content that is in the pages, still you can use felters

how to felter: use BeautifulSoup

for _ in website.run():
	print(BeautifulSoup(_).find_all(class_="Profile"))
	# with that you will felter all the elements that has a class=profiles.
---------------------------------------------------------------------------------------------------------

"""
import requests as req
from bs4 import BeautifulSoup as bs 
from typing import Union
from itertools import chain # flattens a 2D list to a 1D list

class webSiteScrapper:
	"""
	param: n => the number of pages to scrap
	paparam: url => the first page

	url should be the first pages and the next page should be formatted like this
	f"url{pageNumParameter}"
	"""
    def __init__(self, url, n:int=1):
        self.url = url
        self.n = n
        self.is2D: bool
        self.pageNumParameter: str ="?page="
        self.scrapPages()
    def scrap(self, url=None) -> str:
        """
        the first function in the hierarchy
        """
        if url == None:
            return req.get(self.url).content
        else:
            return req.get(url).content

    def scrapPages(self) -> list:
        if self.n == 1:
            self.companyNames1D = [i.a.text for i in bs(self.scrap(), 'html.parser')]
        else:
            self.companyNames2D = [[i.a.text for i in bs(self.scrap(f"{self.url}{self.pageNumParameter}{x}"), 'html.parser')] for x in range(self.n)]
            self.is2D = True
    def extractAllcompanyNames(self) -> list:
        if self.is2D:
            return list(chain.from_iterable([["".join(i.split()) for i in x] for x in self.companyNames2D]))
        else:
            return ["".join(i.split()) for i in self.companyNames2D][0]
    def run(self):
    	return self.extractAllcompanyNames()

# example

pageNum = 3
url = "https://clutch.co/web-developers"

content = webSiteScrapper(url, pageNum) # returns html of all the pages that was specified

# you can pass it then to parser (beautifulsoup(content, 'html.parser')) or loop thro it.
# you can filter the data that you  want.