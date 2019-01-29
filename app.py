import aiohttp
import asyncio
import os
import aiofiles
from io import BytesIO,StringIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
import functools

urls  = [
"http://data.gdeltproject.org/gdeltv2/20150218230000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150218231500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150218233000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150218234500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219000000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219001500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219003000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219010000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219011500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219013000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219014500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219020000.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219021500.gkg.csv.zip",

"http://data.gdeltproject.org/gdeltv2/20150219023000.gkg.csv.zip"]



async def download_coroutine(session, url):
    async with session.get(url) as response:
        filename = os.path.basename(url)
        print("\033[35m" + f"BEGINING : Downloading {filename}" + "\033[0m" )
        binaryfile = BytesIO()
        while True:
            chunk = await response.content.read(1024)
            print("\033[91m" + f"In the process of Downloading {filename}" + "\033[0m" )
            if not chunk:
                break
            binaryfile.write(chunk)
        await response.release()
        loop = asyncio.get_event_loop()
        csvfile = await loop.run_in_executor(e, functools.partial(unzip, binaryfile, filename))
        result = await finder(csvfile)
        return None

def unzip(binarysteam, filename):
    print("\033[36m" + f"BEGINING: Unziping file {filename}" + "\033[0m")
    csvfile = filename[:-4]
    with ZipFile(binarysteam) as zip_ref:
         with zip_ref.open(csvfile) as myfile:
            return myfile.read()

async def finder(filename):
    lst = []
    lstOfSubStrings = ["MEDICAL","SCIENCE","GENERAL_HEALTH","HEALTH_PANDEMIC","HEALTH_SEXTRANSDISEASE","HEALTH_VACCINATION","MEDICAL_SECURITY"]
    async with aiofiles.open(filename) as csvfile:
        pass
        # async for line in csvfile:
        #     subline = line.split("\t")[8]
        #     if any(x in subline for x in lstOfSubStrings):
        #         lst.append(line)
                
    # return len(lst)

async def main(loop):
   
    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [download_coroutine(session, url) for url in urls[0:1]]
        await asyncio.gather(*tasks)

## Create a producer and consumer patterne
## consumer would be process run in excecut that would take elment from queues
if __name__ == '__main__':
    import time
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    e = ThreadPoolExecutor()

    # Download and read list of urls from gdelt data
    s = time.perf_counter()
    loop.run_until_complete(main(loop))
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")