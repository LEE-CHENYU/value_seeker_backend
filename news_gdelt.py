from gdeltdoc import GdeltDoc, Filters

f = Filters(
    keyword = ["APPL", "stock"],
    theme = ["ECON_STOCKMARKET", "ECON_TRADE"],
    domain = ["bloomberg.com", "ft.com", "wsj.com"
              
        #               # Stock Analysis/Screening
        # "finviz.com", "tradingview.com", "stockcharts.com", "barchart.com",
        # "investing.com", "seekingalpha.com", "finance.yahoo.com", "marketwatch.com",
        
        # # Stock Data Providers
        # "marketbeat.com", "tipranks.com", "zacks.com", "morningstar.com",
        # "gurufocus.com", "simplywall.st", "stockanalysis.com",
        
        # # Financial News
        # "benzinga.com", "thestreet.com", "barrons.com", "investors.com",
        # "fool.com", "reuters.com", "bloomberg.com",
        
        # # Stock Research/Ratings
        # "moodys.com", "standardandpoors.com", "fitchratings.com", 
        # "msci.com", "factset.com", "capitaliq.com",
        
        # # SEC Filings & Company Info
        # "sec.gov"
        
              ],
    start_date = "2024-10-14",
    end_date = "2024-11-15"
)

gd = GdeltDoc()
articles = gd.article_search(f)

print(articles)
