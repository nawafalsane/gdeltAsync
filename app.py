import aiohttp
import asyncio
import os
import aiofiles
from io import BytesIO,StringIO
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
import functools
import asyncpg

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



async def download_coroutine(session, url, pool):
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
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.copy_records_to_table('gdelt', records=result)
                # await connection.executemany('''
                # insert into gdelt (GKGRECORDID,DATE,SourceCollectionIdentifier,SourceCommonName,DocumentIdentifier,Counts,V2Counts,Themes,V2Themes,Locations,V2Locations,Persons,V2Persons,Organizations,V2Organizations,V2Tone,Dates,GCAM,SharingImage,RelatedImages,SocialImageEmbeds,SocialVideoEmbeds,Quotations,AllNames,Amounts,TranslationInfo,Extras)
                #  values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27);''', 
                # result)

def unzip(binarysteam, filename):
    print("\033[36m" + f"BEGINING: Unziping file {filename}" + "\033[0m")
    csvfile = filename[:-4]
    with ZipFile(binarysteam) as zip_ref:
         with zip_ref.open(csvfile) as myfile:
            return myfile.readlines()

async def finder(filename):
    lst = []
    lstOfSubStrings = [b"MEDICAL",b"SCIENCE",b"GENERAL_HEALTH",b"HEALTH_PANDEMIC",b"HEALTH_SEXTRANSDISEASE",b"HEALTH_VACCINATION",b"MEDICAL_SECURITY"]
    for line in filename:
        subline = line.split(b"\t")[8]
        if any(x in subline for x in lstOfSubStrings):
            lst.append(tuple(line))
        await asyncio.sleep(0)
                
    return lst

async def main(loop):
    pool = await asyncpg.create_pool(database='nawafalsane', password="1001",user='nawafalsane')
    # Configure service routes
    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [download_coroutine(session, url,pool) for url in urls[0:1]]
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