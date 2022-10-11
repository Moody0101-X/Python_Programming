"""
------------------------------------------------------------------------------------------------------------
author: Moody0101
Prototype:
    This module scraps all the company names from a website called clutch.co (it is a website
    for findind jobs) then generates all the possible valid emails, then puts everything in a csv file
code proto:
    funcList = [scrap, scrapPages, extractAllcompanyNames, EmailGenerator]
    => we have here three funcs, each function serves some purpose.
    scrap: - takes a url then returns the content of the web page.
           - serves scrapPages with the content to scrap every page.
    scrapPages: - uses scrap() to get data from multiple pages.
                - returns a list containing the data from each pages 
                - takes n, the pages number => {the number of pages to be scrapped}
                - takes firtOne, the first page so it can go to other pages easily.
    extractAllcompanyNames: flattens the 2D array that gets returned from scrapPages()
    EmailGenerator: takes the output of extractAllcompanyNames then generates possible valid emails
    using 
    Hierarchy:
    scrap() => scrapPages() => extractAllcompanyNames() => EmailGenerator()
    content => <= pagesNumber | 2D list => 1D list => EmailList

class prototype:
    # I do this every time I will make a class for design purpose
    constructure = {
        urlFirstPages: str = {url}?page=;
        PagesCount: int = 1;
        EmailPrefixes: list[str] = [];
        CompanyNames: list[str] = []
        Emails: list[str] = []
        }
    note: clutch is using captcha, so m pretty much fucked up, so.. but I will try another sulotion.
------------------------------------------------------------------------------------------------------------
"""
import requests as req
from bs4 import BeautifulSoup as bs 
from typing import Union
from itertools import chain
from dataclasses import dataclass

class _clutchwebSiteScrapper:
    def __init__(self, url: str='https://clutch.co/web-developers', n:int=1, m:int=None):
        self.url = url
        self.n = n
        self.m = m
        self.pageNumParameter: str ="?page="  
        self.EmailPrefixes = [
            'social', 'marketing', 'hello', 'contact', 'support', 'info', 'press',
            'media', 'team', 'sales', 'enquiries', 'help', 'business', 'service',
            'career', 'community', 'opportunities', 'accelerate',"hi"
        ]
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
            self.companyNames1D = [i.a.text for i in bs(self.scrap(), 'html.parser').find_all(class_="company_info")]
            self.is2D = False
        else:
            if self.m:
                self.companyNames2D = [[i.a.text for i in bs(self.scrap(f"{self.url}{self.pageNumParameter}{x}"), 'html.parser').find_all(class_="company_info")] for x in range(self.n, self.m)]
            else:
                self.companyNames2D = [[i.a.text for i in bs(self.scrap(f"{self.url}{self.pageNumParameter}{x}"), 'html.parser').find_all(class_="company_info")] for x in range(self.n)]
            self.is2D = True
    def extractAllcompanyNames(self) -> list:
        
        if self.is2D:
            return list(chain.from_iterable([["".join(i.split()) for i in x] for x in self.companyNames2D]))
        else:
            return ["".join(i.split()) for i in self.companyNames1D]
    def run(self):
        return self.extractAllcompanyNames()
        




scrap = _clutchwebSiteScrapper(n=201, m=300) #900 40.000

name = scrap.run()

for _ in name:
    with open("Names.csv", "a") as f:
        try:
            f.write(f'{_}\n')
        except:
            pass
