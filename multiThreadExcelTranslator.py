import xlrd
import xlwt
import time
import math
import queue
import numpy
import concurrent.futures
import pandas as pd
import threading
from google.cloud import translate
from tqdm import tqdm

MAX_WORKERS = 100
sem = threading.Semaphore(MAX_WORKERS)
start = time.time()  
client = translate.TranslationServiceClient()
file_path = input("full path of the document to translate :")

#Writing to file
wb_w = xlwt.Workbook()
#Reading file
wb_r = pd.read_excel(file_path, sheet_name=None, header = None, index_col= None)

#Queue row datas from xlsx
raw_data = queue.Queue()
#Queue for translated datas
translated_data = queue.Queue()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

futures = []

def detect_language(text, project_id="peppy-freedom-317516"):
    """Detecting the language of a text string."""

    location = "global"

    parent = f"projects/{project_id}/locations/{location}"

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    return client.detect_language(
        content=text,
        parent=parent,
        mime_type="text/plain",  # mime types: text/plain, text/html
    ).languages[0]


def translate_text(text, project_id="peppy-freedom-317516"):
    """Translating Text."""

    location = "global"

    parent = f"projects/{project_id}/locations/{location}"

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    return client.translate_text(
        request={
            "parent": parent,
            "contents": [text],
            "mime_type": "text/plain",  # mime types: text/plain, text/html
            "source_language_code": "ko",
            "target_language_code": "en",
        }
    ).translations[0]
    
def process_cell(row, column, value):
	sem.acquire()
	response = detect_language(value)
	if (response.language_code == "ko" and response.confidence > 0.9):
		value = translate_text(value).translated_text
	translated_data.put({ "row" : row, "col" : column, "value" : value })
	sem.release()

def feed_raw_data_into_queues(sheet):
	sem.acquire()
	for row_index in range(sheet.shape[0]):
		futures = []
		for col_index in range(sheet.shape[1]):
			value = sheet.iat[row_index, col_index]
			if type(value) == str:
				raw_data.put({ "row" : row_index, "col" : col_index, "value" : value })
			elif type(value) == float:
				if math.isnan(value):
					translated_data.put({ "row" : row_index, "col" : col_index, "value" : "" })
				else:
					translated_data.put({ "row" : row_index, "col" : col_index, "value" : value })
			elif type(value) == numpy.int64 or type(value) == int:
				translated_data.put({ "row" : row_index, "col" : col_index, "value" : value })
			else:
				translated_data.put({ "row" : row_index, "col" : col_index, "value" : value })
	sem.release()
			
			
def feed_workers():
	sem.acquire()
	while not data_spliter.done() or not raw_data.empty():
		while not raw_data.empty():
			data = raw_data.get()
			futures.append(executor.submit(process_cell, row=data["row"], column=data["col"], value=data["value"]))
	sem.release()

for name, sheet in wb_r.items():
	sheet_to_write = wb_w.add_sheet(name)
	data_spliter = executor.submit(feed_raw_data_into_queues, sheet=sheet)
	data_feeder = executor.submit(feed_workers)
	futures = []
	bar_title = "'" + name + "' is being translated"
	pbar = tqdm(total=sheet.size, desc=bar_title , position=0, leave=True)
	#Trying to find a wat to remove this sleep, the semaphore are not acquired yet in the data_feeder and data_spliter, must wait a small time before being sure it acquired them
	time.sleep(0.1)
	while sem._value != MAX_WORKERS or not translated_data.empty():
		while not translated_data.empty():
			data = translated_data.get()
			sheet_to_write.write(data["row"], data["col"], str(data["value"]))
			pbar.update(1)
		#This sleep is not dirty, it allows us to treat only a chunck of data at a time and not blocking the queue by constant unstacking
		time.sleep(0.1)
	pbar.close()


translated_filename = file_path.split('.')[0] + "_translated.xlsx"
wb_w.save(translated_filename)
end = time.time()
print("took : " +  str(end - start) +  " second(s)")



