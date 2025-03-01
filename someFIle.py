import yellowpages
def main():
    myYP = yellowpages.YellowPageScraper("restaurant", "New York")
    outputPath = myYP.scrape_all_pages()
    yellowpages.find_emails(outputPath)
    
if __name__ == "__main__":
    main()