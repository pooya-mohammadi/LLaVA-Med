from os.path import split, join
import json
import os
import asyncio

import aiohttp
from bs4 import BeautifulSoup
from deep_utils import JsonUtils

main_link = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package"


async def crawl_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            page_text = await response.text()
            soup = BeautifulSoup(page_text, 'html.parser')
            return soup


async def main():
    main_page = await crawl_page(main_link)
    links = [join(main_link, a.get("href")) for a in main_page.find_all('a') if len(a.get("href")) == 3]
    pmc_links = []
    await asyncio.gather(*[get_pmc_links(link, pmc_links) for link in links])
    JsonUtils.dump_json("pmc_page_links.json", pmc_links)
    pmc_links = JsonUtils.load_json("pmc_page_links.json")
    print(f"number of links: {len(pmc_links)}")
    final_output = dict()
    await asyncio.gather(*[get_final_pmc_links(final_output, pmc_link) for pmc_link in pmc_links])
    return final_output

async def get_final_pmc_links(final_output, pmc_link):
    while True:
        try:
            page = await crawl_page(pmc_link)
            download_links = [join(pmc_link, a.get("href")) for a in page.find_all('a') if
                              a.get("href").endswith(".tar.gz")]
            for link in download_links:
                pmc_id = split(link)[-1].replace(".tar.gz", "")
                final_output[pmc_id] = link
            break
        except:
            continue


async def get_pmc_links(link, pmc_links):
    while True:
        try:
            page = await crawl_page(link)
            download_links = [join(link, a.get("href")) for a in page.find_all('a') if len(a.get("href")) == 3]
            pmc_links.extend(download_links)
            break
        except:
            continue


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    output = asyncio.run(main())
    JsonUtils.dump_json("all_pmc.json", output)
