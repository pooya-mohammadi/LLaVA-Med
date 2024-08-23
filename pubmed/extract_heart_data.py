from multiprocessing import Lock
from multiprocessing.pool import ThreadPool
from os.path import split, exists, join
from tqdm import tqdm
from deep_utils import JsonUtils
from selenium import webdriver
from selenium.webdriver.common.by import By

from selenium.webdriver.common.keys import Keys

search_keyword = "heart"
url_path = f"https://www.ncbi.nlm.nih.gov/pmc/?term={search_keyword}"
number_of_pages = 100000
number_of_pools = 10
save_counter = 100
ids_dict = JsonUtils.load_json("ids.json")
failed_pages_path = f"{search_keyword}_done_pages.json"
done_pages_path= f"{search_keyword}_failed_pages.json"
output_path = search_keyword + ".json"
done_pages = set(JsonUtils.load_json(done_pages_path)) if exists(done_pages_path) else set()
failed_pages = set(JsonUtils.load_json(failed_pages_path)) if exists(failed_pages_path) else set()
output_file = JsonUtils.load_json(output_path) if exists(output_path) else set()

lock = Lock()


def main(page_count: int):
    try:
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        options.add_argument("--incognito")
        options.add_argument("--nogpu")
        options.add_argument("--disable-gpu")
        # options.add_argument("--window-size=1280,1280")
        options.add_argument("--no-sandbox")
        options.add_argument("--enable-javascript")
        # options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # options.add_experimental_option('useAutomationExtension', False)
        # options.add_argument('--disable-blink-features=AutomationControlled')
        service = webdriver.FirefoxService(executable_path=f'{split(__file__)[0]}/geckodriver')
        driver = webdriver.Firefox(service=service, options=options)
        driver.get(url_path)
        driver.execute_script(
            f"document.getElementById('EntrezSystem2.PEntrez.PMC.Pmc_ResultsPanel.Entrez_Pager.Page').setAttribute('page', '{page_count}')")
        next_button = driver.find_element(by=By.ID,
                                          value="EntrezSystem2.PEntrez.PMC.Pmc_ResultsPanel.Entrez_Pager.Page")
        next_button.click()
        # next_button.
        # v = 10
        output = driver.find_elements(by=By.CLASS_NAME, value="rprt")
        files = []
        for element in output:
            link = element.find_element(by=By.CLASS_NAME, value="links")
            views = link.find_elements(by=By.CLASS_NAME, value="view")
            view = views[-1]
            final_link = view.get_property("href")
            pmc_id = [item for item in final_link.split("/") if "PMC" in item][0]
            try:
                files.append((final_link, pmc_id, "https://ftp.ncbi.nlm.nih.gov/pub/pmc/" + ids_dict[pmc_id][0],
                              ids_dict[pmc_id][1]))
            except:
                files.append((final_link, pmc_id, "", ""))
        output_file.extend(files)
        done_pages.add(page_count)
    except Exception as e:
        failed_pages.add(page_count)
        print(f"[INFO] failed to do page_count: {page_count}, error: {e}")


if __name__ == '__main__':
    threads = []
    with ThreadPool(number_of_pools) as pool:
        for i in tqdm(range(number_of_pages)):
            if i in done_pages:
                continue
            threads.append(pool.apply_async(main, args=(i,)))

        for index, thread in tqdm(enumerate(threads), total=len(threads)):
            thread.get()
            if index % save_counter == 0:
                lock.acquire()
                JsonUtils.dump_json(done_pages_path, list(done_pages))
                JsonUtils.dump_json(failed_pages_path, list(failed_pages))
                JsonUtils.dump_json(output_path, output_file)
                lock.release()
    # driver.close()
